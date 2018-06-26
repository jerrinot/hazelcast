package com.hazelcast.streamer.impl;

import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.StreamerConfig;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.Disposable;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.Data;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.Math.abs;
import static java.util.Collections.binarySearch;

public final class DummyStore implements Disposable {
    private static final int RECORD_HEADER_SIZE = 8 + 4; //8 = offset for checksum (long), 4 = record size in bytes (int)

    private final File dir;
    private final String name;
    private final int partitionId;
    private final int chunkSizeBytes;

    private final ByteBuffer memoryBuffer;
    private final List<Long> startingOffsetInFiles = new ArrayList<Long>();

    private RandomAccessFile currentChunk;
    private long bytesInTheCurrentChunk;

    private long memoryOffsetStart;
    private long highWatermark;

    private long maximumWriteLatencyNanos = 0;

    public DummyStore(String name, int partitionId, int totalPartitionCount, StreamerConfig streamerConfig) {
        checkNotNull("Streamer " + name + " has no overflow directory configured", streamerConfig.getOverflowDir());

        this.name = name;
        this.partitionId = partitionId;
        File storeDir = new File(streamerConfig.getOverflowDir());
        if (!storeDir.isDirectory()) {
            throw new ConfigurationException("Streamer " + name + " has a wrong overflow directory configured: " + storeDir);
        }
        //todo: if the directory exists then check it's empty
        this.dir = new File(storeDir, Integer.toString(partitionId));
        if (!dir.exists()) {
            boolean mkdirs = dir.mkdirs();
            assert mkdirs;
        }

        int bufferSizeBytes = (streamerConfig.getMaxSizeInMemoryMB() * 1024 * 1024) / totalPartitionCount;
        this.memoryBuffer = ByteBuffer.allocateDirect(bufferSizeBytes);

        this.chunkSizeBytes = streamerConfig.getChunkSizeMB() * 1024 * 1024;
    }

    public void add(Data value) {
        if (!addToMemoryStore(value)) {
            writeCurrentBufferToDisk();
            assert addToMemoryStore(value);
        }
    }

    private boolean addToMemoryStore(Data data) {
        int dataSizeBytes = data.totalSize();
        int totalSizeRequiredBytes = RECORD_HEADER_SIZE + dataSizeBytes;

        if (totalSizeRequiredBytes > memoryBuffer.capacity()) {
            //todo: better message
            throw new ConfigurationException("too big");
        }

        int remainingCapacityBytes = memoryBuffer.remaining();
        if (remainingCapacityBytes >= totalSizeRequiredBytes) {
            memoryBuffer.putLong(highWatermark);
            memoryBuffer.putInt(dataSizeBytes);
            memoryBuffer.put(data.toByteArray());
            highWatermark += totalSizeRequiredBytes;
            return true;
        }
        return false;
    }

    private void writeCurrentBufferToDisk() {
        long bytesToBeWritten = highWatermark - memoryOffsetStart;
        if (currentChunk == null || bytesToBeWritten + bytesInTheCurrentChunk > chunkSizeBytes) {
            closeResource(currentChunk);
            try {
                currentChunk = new RandomAccessFile(fileForOffset(memoryOffsetStart), "rw");
            } catch (FileNotFoundException e) {
                throw new IllegalStateException("cannot create a new chunk", e);
            }
            startingOffsetInFiles.add(memoryOffsetStart);
            bytesInTheCurrentChunk = 0;
        }

        memoryBuffer.flip();
        try {
            long startingTimeNanos = System.nanoTime();
            int writtenBytes = currentChunk.getChannel().write(memoryBuffer);
            long writtingTimeNanos = System.nanoTime() - startingTimeNanos;
            if (writtingTimeNanos > maximumWriteLatencyNanos) {
                maximumWriteLatencyNanos = writtingTimeNanos;
                System.out.println("Write latency: " + TimeUnit.NANOSECONDS.toMicros(writtingTimeNanos) + " micros");
            }
            bytesInTheCurrentChunk += writtenBytes;
            assert writtenBytes == bytesToBeWritten;
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("Cannot write streamer file to a disk", e);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot write streamer file to a disk", e);
        }
        memoryOffsetStart = highWatermark;
        memoryBuffer.clear();
    }

    public int read(long currentOffset, final int maxRecords, final InternalConsumer consumer) {
        //todo: refactor this mess!
        int entriesRead = 0;
        while (entriesRead < maxRecords) {
            if (currentOffset >= memoryOffsetStart) {
                //fast path - reading from memory
                return readFromMemory(currentOffset, entriesRead, maxRecords, consumer);
            } else {
                long fileStartingOffset = findStartingOffset(currentOffset);
                RandomAccessFile raf = null;
                File offsetFile = fileForOffset(fileStartingOffset);
                try {
                    raf = new RandomAccessFile(offsetFile, "rw");
                    int offsetInsideFile = (int) (currentOffset - fileStartingOffset);
                    do {
                        raf.seek(offsetInsideFile);
                        long checksumOffset = raf.readLong();
                        offsetInsideFile += 8;
                        raf.seek(offsetInsideFile);
                        if (checksumOffset != currentOffset) {
                            throw new IllegalStateException("corrupted store, expected: " + currentOffset + ", found: " + checksumOffset);
                        }
                        int recordSize = raf.readInt();
                        offsetInsideFile += 4;
                        raf.seek(offsetInsideFile);
                        byte[] buffer = new byte[recordSize];
                        raf.readFully(buffer);
                        Data data = new HeapData(buffer);
                        long nextRecordOffset = currentOffset + recordSize + RECORD_HEADER_SIZE;
                        consumer.accept(partitionId, currentOffset, data, nextRecordOffset);

                        offsetInsideFile += recordSize;
                        currentOffset = nextRecordOffset;
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
        }
        return entriesRead;
    }

    private int readFromMemory(long currentOffset, int entriesRead, int maxRecords, InternalConsumer consumer) {
        for (; entriesRead < maxRecords && currentOffset < highWatermark; entriesRead++) {
            int offsetInsideBuffer = (int) (currentOffset - memoryOffsetStart);
            long checksumOffset = memoryBuffer.getLong(offsetInsideBuffer);
            if (checksumOffset != currentOffset) {
                throw new IllegalStateException("corrupted store, expected: " + currentOffset + ", found: " + checksumOffset);
            }
            int recordSize = memoryBuffer.getInt(offsetInsideBuffer + 8);
            byte[] recordBuffer = new byte[recordSize];
            int origPosition = memoryBuffer.position();
            try {
                memoryBuffer.position(offsetInsideBuffer + RECORD_HEADER_SIZE);
                memoryBuffer.get(recordBuffer);
            } finally {
                memoryBuffer.position(origPosition);
            }

            Data data = new HeapData(recordBuffer);
            long nextRecordOffset = currentOffset + recordSize + RECORD_HEADER_SIZE;
            consumer.accept(partitionId, currentOffset, data, nextRecordOffset);
            currentOffset = nextRecordOffset;
        }
        return entriesRead;
    }

    private File fileForOffset(long startingOffset) {
        return new File(dir, filenameForOffset(startingOffset));
    }

    private String filenameForOffset(long startingOffset) {
        return Long.toString(startingOffset);
    }

    private long findStartingOffset(long offset) {
        int insertionPoint = binarySearch(startingOffsetInFiles, offset);
        return insertionPoint < 0 ? startingOffsetInFiles.get(abs(insertionPoint) - 2) : startingOffsetInFiles.get(insertionPoint);
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getName() {
        return name;
    }

    @Override
    public void dispose() {
        closeResource(currentChunk);
        for (long startingOffset : startingOffsetInFiles) {
            File file = fileForOffset(startingOffset);
            file.delete();
        }
        startingOffsetInFiles.clear();
        memoryOffsetStart = 0;
        memoryBuffer.clear();
        highWatermark = 0;
    }


    /*************************************************************
      SERIALIZATION RELATED STUFF BELLOW. THIS WILL CHANGE SOON
     ***********************************************************/
    public void savePayload(BufferObjectDataOutput bodo) throws IOException {
        bodo.writeInt(startingOffsetInFiles.size());
        for (long startingOffsetInFile : startingOffsetInFiles) {
            bodo.writeLong(startingOffsetInFile);
            File file = fileForOffset(startingOffsetInFile);
            FileInputStream fis = null;
            try {
                fis = new FileInputStream(file);
                byte[] bytes = IOUtil.toByteArray(fis);
                bodo.writeByteArray(bytes);
            } finally {
                closeResource(fis);
            }
        }

        bodo.writeLong(memoryOffsetStart);
        int origPosition = memoryBuffer.position();
        bodo.writeInt(memoryBuffer.position());
        memoryBuffer.flip();
        while (memoryBuffer.hasRemaining()) {
            bodo.writeByte(memoryBuffer.get());
        }
        memoryBuffer.position(origPosition);
        memoryBuffer.limit(memoryBuffer.capacity());
    }

    public void restorePayload(ObjectDataInput in) throws IOException {
        int fileCount = in.readInt();
        for (int i = 0; i < fileCount; i++) {
            long startingOffset = in.readLong();
            byte[] bytes = in.readByteArray();
            startingOffsetInFiles.add(startingOffset);
            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(fileForOffset(startingOffset));
                fos.write(bytes);
            } finally {
                closeResource(fos);
            }
        }

        memoryBuffer.clear();

        memoryOffsetStart = in.readLong();
        int bytes = in.readInt();
        highWatermark = memoryOffsetStart + bytes;
        for (int i = 0; i < bytes; i++) {
            byte b = in.readByte();
            memoryBuffer.put(b);
        }
    }
}
