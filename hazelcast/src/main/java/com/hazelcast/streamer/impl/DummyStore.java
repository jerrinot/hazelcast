package com.hazelcast.streamer.impl;

import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.StreamerConfig;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.Disposable;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.streamer.JournalValue;
import com.hazelcast.util.Preconditions;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.Math.abs;
import static java.util.Collections.binarySearch;

public final class DummyStore implements Disposable {
    private final File dir;

    private final String name;
    private final int partitionId;

    private final List<Data> values = new ArrayList<Data>();
    private final List<Long> offsetsInFiles = new ArrayList<Long>();
    private final int inMemoryMaxEntries;
    private long memoryOffsetStart;
    private long bytesInMemory;

    public DummyStore(String name, int partitionId, StreamerConfig streamerConfig) {
        checkNotNull("Streamer " + name + " has no overflow directory configured", streamerConfig.getOverflowDir());

        this.name = name;
        this.partitionId = partitionId;
        File storeDir = new File(streamerConfig.getOverflowDir());
        if (!storeDir.isDirectory()) {
            throw new ConfigurationException("Streamer " + name + " has a wrong overflow directory configured: " + storeDir);
        }
        this.dir = new File(storeDir, Integer.toString(partitionId));
        if (!dir.exists()) {
            boolean mkdirs = dir.mkdirs();
            assert mkdirs;
        }
        this.inMemoryMaxEntries = streamerConfig.getMaxSizeInMemory();
        //todo: if the directory exists then check it's empty
    }

    public void add(Data value) {
        addToMemoryStore(value);
        if (isMemoryFull()) {
            writeCurrentBufferToDisk();
        }
    }

    private void addToMemoryStore(Data data) {
        bytesInMemory += data.totalSize();
        values.add(data);
    }

    private boolean isMemoryFull() {
        return values.size() == inMemoryMaxEntries;
    }

    private void writeCurrentBufferToDisk() {
        int entryCount = values.size();
        long fileSize = 4 // entry count
                + entryCount * 4 // directory
                + entryCount * 4 // entry size
                + bytesInMemory;
        File file = fileForOffset(memoryOffsetStart);
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            MappedByteBuffer buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            writeHeader(entryCount, buffer);
            writePayload(buffer);
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("Cannot write streamer file to a disk", e);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot write streamer file to a disk", e);
        } finally {
            closeResource(raf);
        }
        offsetsInFiles.add(memoryOffsetStart);
        values.clear();
        bytesInMemory = 0;
        memoryOffsetStart += entryCount;
    }

    private void writePayload(MappedByteBuffer buffer) {
        for (Data data : values) {
            byte[] bytes = data.toByteArray();
            buffer.putInt(bytes.length);
            buffer.put(bytes);
        }
    }

    private void writeHeader(int entryCount, MappedByteBuffer buffer) {
        buffer.putInt(entryCount);
        int offsetInFile = getHeaderSize(entryCount);
        for (Data data : values) {
            buffer.putInt(offsetInFile);
            offsetInFile += data.toByteArray().length;
            offsetInFile += 4;
        }
    }

    private int getHeaderSize(int entryCount) {
        return 4 + (entryCount * 4);
    }

    public boolean hasEnoughRecordsToRead(long offset, int minRecords) {
        long size = getSize();
        return size - offset >= minRecords;
    }

    public int read(long curentOffset, final int maxRecords, PollResult response) {
        //todo: refactor this mess!
        int entriesRead = 0;
        List<JournalValue<Data>> results = response.getResults();
        do {
            if (curentOffset >= memoryOffsetStart) {
                //fast path - reading from memory
                long size = getSize();
                for (; entriesRead < maxRecords && curentOffset < size; entriesRead++) {
                    int index = (int) (curentOffset - memoryOffsetStart);
                    Data value = values.get(index);
                    JournalValue<Data> journalValue = new JournalValue<Data>(value, index, partitionId);
                    results.add(journalValue);
                    curentOffset++;
                }
                response.setNextSequence(curentOffset);
                return entriesRead;
            } else {
                long startingOffset = findStartingOffset(curentOffset);
                RandomAccessFile raf = null;
                File offsetFile = fileForOffset(startingOffset);
                try {
                    raf = new RandomAccessFile(offsetFile, "rw");
                    long headerOffsetWithinFile = (4 * curentOffset) - (4 * startingOffset) + 4;
                    raf.seek(headerOffsetWithinFile);
                    int payloadOffsetWithinFile = raf.readInt();
                    raf.seek(payloadOffsetWithinFile);
                    do {
                        JournalValue<Data> journalValue = readSingleRecord(curentOffset, raf);
                        results.add(journalValue);
                        curentOffset++;
                        response.setNextSequence(curentOffset);
                        entriesRead++;
                    } while (raf.getFilePointer() != raf.length() && entriesRead < maxRecords);
                } catch (FileNotFoundException e) {
                    throw new IllegalStateException("Error while opening offset file", e);
                } catch (IOException e) {
                    throw new IllegalStateException("Error while reading offset file", e);
                } finally {
                    closeResource(raf);
                }
            }
        } while (entriesRead < maxRecords);
        return entriesRead;
    }

    private File fileForOffset(long startingOffset) {
        return new File(dir, filenameForOffset(startingOffset));
    }

    private String filenameForOffset(long startingOffset) {
        return Long.toString(startingOffset);
    }

    private long findStartingOffset(long offset) {
        int insertionPoint = binarySearch(offsetsInFiles, offset);
        return insertionPoint < 0 ? offsetsInFiles.get(abs(insertionPoint) - 2) : offsetsInFiles.get(insertionPoint);
    }

    private JournalValue<Data> readSingleRecord(long offset, RandomAccessFile raf) throws IOException {
        int payloadSize = raf.readInt();
        byte[] buffer = new byte[payloadSize];
        raf.readFully(buffer);
        Data data = new HeapData(buffer);
        return new JournalValue<Data>(data, offset, partitionId);
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getName() {
        return name;
    }

    private long getSize() {
        return memoryOffsetStart + values.size();
    }

    public void savePayload(BufferObjectDataOutput bodo) throws IOException {
        writeAllDiskFiles(bodo);
        writeMemoryData(bodo);
    }

    private void writeAllDiskFiles(BufferObjectDataOutput out) throws IOException {
        out.writeInt(offsetsInFiles.size());
        for (long fileOffset : offsetsInFiles) {
            writeSingleFile(out, fileOffset);
        }
    }

    private void writeSingleFile(BufferObjectDataOutput out, long fileOffset) throws IOException {
        File file = fileForOffset(fileOffset);
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
            out.writeByteArray(IOUtil.toByteArray(fis));
        } finally {
            closeResource(fis);
        }
    }

    private void writeMemoryData(ObjectDataOutput out) throws IOException {
        out.writeLong(memoryOffsetStart);
        out.writeInt(values.size());
        for (Data data : values) {
            out.writeData(data);
        }
    }

    public void restorePayload(ObjectDataInput in) throws IOException {
        readDiskData(in);
        readMemoryData(in);
    }

    private void readDiskData(ObjectDataInput in) throws IOException {
        int filesCount = in.readInt();
        long currentFileOffset = 0;
        for (int i = 0; i < filesCount; i++) {
            offsetsInFiles.add(currentFileOffset);
            byte[] buffer = in.readByteArray();
            File file = fileForOffset(currentFileOffset);
            RandomAccessFile raf = null;
            try {
                raf = new RandomAccessFile(file, "rw");
                raf.write(buffer);
                raf.seek(0);
                int entryCount = raf.readInt();
                currentFileOffset += entryCount;
            } finally {
                closeResource(raf);
            }
        }
    }

    private void readMemoryData(ObjectDataInput in) throws IOException {
        memoryOffsetStart = in.readLong();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Data data = in.readData();
            addToMemoryStore(data);
        }
    }

    @Override
    public void dispose() {
        for (long startingOffset : offsetsInFiles) {
            File file = fileForOffset(startingOffset);
            file.delete();
        }
        offsetsInFiles.clear();
        memoryOffsetStart = 0;
        values.clear();
        bytesInMemory = 0;
    }
}
