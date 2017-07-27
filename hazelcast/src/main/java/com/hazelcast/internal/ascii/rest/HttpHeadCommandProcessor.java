package com.hazelcast.internal.ascii.rest;

import com.hazelcast.internal.ascii.TextCommandService;

public class HttpHeadCommandProcessor extends HttpCommandProcessor<HttpHeadCommand> {

    public HttpHeadCommandProcessor(TextCommandService textCommandService) {
        super(textCommandService);
    }

    @Override
    public void handle(HttpHeadCommand command) {
        String uri = command.getURI();
        if (uri.startsWith(URI_MAPS)) {
            command.send200();
        } else if (uri.startsWith(URI_QUEUES)) {
            command.send200();
        } else if (uri.startsWith(URI_CLUSTER)) {
            command.send200();
        } else if (uri.equals(URI_HEALTH_URL)) {
            command.send200();
        } else if (uri.startsWith(URI_CLUSTER_VERSION_URL)) {
            command.send200();
        } else {
            command.send400();
        }
        textCommandService.sendResponse(command);
    }

    @Override
    public void handleRejection(HttpHeadCommand command) {
        handle(command);
    }

}