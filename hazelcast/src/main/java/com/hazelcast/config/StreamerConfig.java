package com.hazelcast.config;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class StreamerConfig implements IdentifiedDataSerializable {
    private static final int DEFAULT_MAX_IN_MEMORY_MB = 10;

    private String name;
    private int maxSizeInMemoryMB = DEFAULT_MAX_IN_MEMORY_MB;
    private String overflowDir;

    public StreamerConfig(StreamerConfig defConfig) {
        this.name = defConfig.name;
        this.maxSizeInMemoryMB = defConfig.maxSizeInMemoryMB;
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

    public int getMaxSizeInMemoryMB() {
        return maxSizeInMemoryMB;
    }

    public StreamerConfig setMaxSizeInMemoryMB(int maxSizeInMemoryMB) {
        this.maxSizeInMemoryMB = maxSizeInMemoryMB;
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
        out.writeInt(maxSizeInMemoryMB);
        out.writeUTF(overflowDir);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        maxSizeInMemoryMB = in.readInt();
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
