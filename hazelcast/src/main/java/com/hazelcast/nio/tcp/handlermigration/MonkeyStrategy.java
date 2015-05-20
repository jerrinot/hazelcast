package com.hazelcast.nio.tcp.handlermigration;

import com.hazelcast.nio.tcp.MigratableHandler;

import java.util.Iterator;
import java.util.Random;
import java.util.Set;

public class MonkeyStrategy extends MigrationStrategy {

    private Random random = new Random();

    @Override
    boolean imbalanceDetected(LoadImbalance imbalance) {
        Set<? extends MigratableHandler> candidates = imbalance.getHandlersOwnerBy(imbalance.sourceSelector);
        return (candidates.size() > 0);
    }

    @Override
    MigratableHandler findHandlerToMigrate(LoadImbalance imbalance) {
        Set<? extends MigratableHandler> candidates = imbalance.getHandlersOwnerBy(imbalance.sourceSelector);
        int handlerCount = candidates.size();
        int selected = random.nextInt(handlerCount);
        Iterator<? extends MigratableHandler> iterator = candidates.iterator();
        for (int i = 0; i < selected; i++) {
            iterator.next();
        }
        return iterator.next();
    }
}
