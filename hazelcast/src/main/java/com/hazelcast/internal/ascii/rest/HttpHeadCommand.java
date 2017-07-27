package com.hazelcast.internal.ascii.rest;

import com.hazelcast.internal.ascii.TextCommandConstants;

import java.nio.ByteBuffer;

public class HttpHeadCommand extends HttpCommand {
    private boolean nextLine;

    public HttpHeadCommand(String uri) {
        super(TextCommandConstants.TextCommandType.HTTP_HEAD, uri);
    }

    @Override
    public boolean readFrom(ByteBuffer src) {
        while (src.hasRemaining()) {
            char c = (char) src.get();
            if (c == '\n') {
                if (nextLine) {
                    return true;
                }
                nextLine = true;
            } else if (c != '\r') {
                nextLine = false;
            }
        }
        return false;
    }
}
