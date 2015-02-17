package com.hazelcast.map.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.Collections;

public class LoadKeysOperationFactory implements OperationFactory {

    private String name;

    public LoadKeysOperationFactory() {
    }

    public LoadKeysOperationFactory(String name) {
        this.name = name;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    public Operation createOperation() {
        return new LoadKeysOperation(name, Collections.<Data>emptyList(), true);
    }

}
