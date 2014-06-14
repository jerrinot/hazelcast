package com.hazelcast.spi.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.UUID;

public class TraceableIsStillExecutingOperation extends AbstractOperation {

    private String serviceName;
    private UUID identifier;

    TraceableIsStillExecutingOperation() {
    }

    public TraceableIsStillExecutingOperation(String serviceName, UUID identifier) {
        this.serviceName = serviceName;
        this.identifier = identifier;
    }

    @Override
    public void run() throws Exception {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        BasicOperationService operationService = (BasicOperationService) nodeEngine.operationService;
        boolean executing = operationService.isOperationExecuting(getCallerAddress(), getCallerUuid(),
                serviceName, identifier);
        getResponseHandler().sendResponse(executing);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        serviceName = in.readUTF();
        long leastSig = in.readLong();
        long mostSig = in.readLong();
        identifier = new UUID(mostSig, leastSig);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(serviceName);
        out.writeLong(identifier.getLeastSignificantBits());
        out.writeLong(identifier.getMostSignificantBits());
    }
}
