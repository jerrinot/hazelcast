package com.hazelcast.config;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class StreamerConfig implements IdentifiedDataSerializable {
    private static final int DEFAULT_MAX_IN_MEMORY_ENTRIES = 1024;

    private String name;
    private int maxSizeInMemory = DEFAULT_MAX_IN_MEMORY_ENTRIES;
    private String overflowDir;

    public StreamerConfig(StreamerConfig defConfig) {
        this.name = defConfig.name;
        this.maxSizeInMemory = defConfig.maxSizeInMemory;
        this.overflowDir = defConfig.overflowDir;
    }

    public StreamerConfig() {

    }

    public String getName() {
        return name;
    }

    public StreamerConfig setName(String name) {
        this.name = name;
        return this;
    }

    public int getMaxSizeInMemory() {
        return maxSizeInMemory;
    }

    public StreamerConfig setMaxSizeInMemory(int maxSizeInMemory) {
        this.maxSizeInMemory = maxSizeInMemory;
        return this;
    }

    public String getOverflowDir() {
        return overflowDir;
    }

    public StreamerConfig setOverflowDir(String overflowDir) {
        this.overflowDir = overflowDir;
        return this;
    }

    public StreamerConfig getAsReadOnly() {
        //todo
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(maxSizeInMemory);
        out.writeUTF(overflowDir);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        maxSizeInMemory = in.readInt();
        overflowDir = in.readUTF();
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.STREAMER_CONFIG;
    }
}
