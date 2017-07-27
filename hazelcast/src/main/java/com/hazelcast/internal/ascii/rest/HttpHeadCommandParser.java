package com.hazelcast.internal.ascii.rest;

import com.hazelcast.internal.ascii.CommandParser;
import com.hazelcast.internal.ascii.TextCommand;
import com.hazelcast.internal.ascii.memcache.ErrorCommand;
import com.hazelcast.nio.ascii.TextChannelInboundHandler;

import java.util.StringTokenizer;

import static com.hazelcast.internal.ascii.TextCommandConstants.TextCommandType.ERROR_CLIENT;

public class HttpHeadCommandParser implements CommandParser {

    @Override
    public TextCommand parser(TextChannelInboundHandler readHandler, String cmd, int space) {
        StringTokenizer st = new StringTokenizer(cmd);
        st.nextToken();
        String uri;
        if (st.hasMoreTokens()) {
            uri = st.nextToken();
        } else {
            return new ErrorCommand(ERROR_CLIENT);
        }
        return new HttpHeadCommand(uri);
    }
}
