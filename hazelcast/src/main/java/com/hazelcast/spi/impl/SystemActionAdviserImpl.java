/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl;

import com.hazelcast.instance.NodeExtension;
import com.hazelcast.memory.GarbageCollectorStats;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.SystemActionAdviser;
import com.hazelcast.util.Clock;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class SystemActionAdviserImpl implements SystemActionAdviser {

    private static final float PERCENTAGE = 100.0F;

    private final NodeEngineImpl nodeEngine;
    private final ExecutionService executionService;
    private final NodeExtension nodeExtension;
    private ScheduledFuture systemStateCheckerScheduler;
    private final SystemState systemState = new SystemState();
    private final boolean enabled;
    private final int checkFreqSeconds;
    private final float gcStwPercentage;
    private final float gcStwDelta;

    SystemActionAdviserImpl(NodeEngineImpl nodeEngine, ExecutionService executionService) {
        this.nodeEngine = nodeEngine;
        this.executionService = executionService;
        this.nodeExtension = nodeEngine.getNode().getNodeExtension();
        this.enabled = nodeEngine.getGroupProperties().SYSTEM_ADVISER_ENABLED.getBoolean();
        this.checkFreqSeconds = nodeEngine.getGroupProperties().SYSTEM_ADVISER_CHECK_FREQUENCY_SECONDS.getInteger();
        this.gcStwPercentage = nodeEngine.getGroupProperties().SYSTEM_ADVISER_GC_STW_PERCENTAGE.getFloat();
        this.gcStwDelta = nodeEngine.getGroupProperties().SYSTEM_ADVISER_GC_STW_DELTA.getFloat();
    }

    @Override
    public boolean isSystemStateOK() {
        if (!enabled) {
            return true;
        }
        return checkSystemState();
    }

    @Override
    public boolean shouldForceSyncBackups() {
        if (!enabled) {
            return false;
        }
        return !checkSystemState();
    }

    protected boolean checkSystemState() {
        long now = Clock.currentTimeMillis();
        // System is under high load and system state cannot be updated in time.
        // So this means that system state is NOT OK.
        if (now - systemState.lastUpdateTime > ((2 * checkFreqSeconds + 1) * 1000)) {
            systemState.ok = false;
        }
        return systemState.ok;
    }

    protected void updateSystemState() {
        long updateTimeDiff = systemState.lastUpdateTime - systemState.prevUpdateTime;
        long totalGcTimeDiff = systemState.lastTotalGcTime - systemState.prevTotalGcTime;
        if (((totalGcTimeDiff * PERCENTAGE) / updateTimeDiff) > gcStwPercentage + gcStwDelta) {
            // GC STW takes more time then threshold, so system state is NOT OK
            systemState.ok = false;
        } else if (((totalGcTimeDiff * PERCENTAGE) / updateTimeDiff) < gcStwPercentage - gcStwDelta) {
            // GC STW takes less time then threshold, so system state is OK
            systemState.ok = true;
        }
    }

    @Override
    public void start() {
        if (enabled) {
            systemStateCheckerScheduler =
                    executionService.scheduleAtFixedRate(
                            "SystemActionAdviser:systemStateChecker",
                            new SystemStateChecker(),
                            checkFreqSeconds,
                            checkFreqSeconds,
                            TimeUnit.SECONDS);
        }
    }

    @Override
    public void shutdown() {
        if (systemStateCheckerScheduler != null) {
            systemStateCheckerScheduler.cancel(true);
        }
    }

    private class SystemStateChecker implements Runnable {

        @Override
        public void run() {
            MemoryStats memoryStats = nodeExtension.getMemoryStats();
            GarbageCollectorStats gcStats = memoryStats.getGCStats();
            long updateTime = Clock.currentTimeMillis();
            long minorGcTime = gcStats.getMinorCollectionTime();
            long majorGcTime = gcStats.getMajorCollectionTime();
            long unknownGcTime = gcStats.getUnknownCollectionTime();
            systemState.prevUpdateTime = systemState.lastUpdateTime;
            systemState.prevMinorGcTime = systemState.lastMinorGcTime;
            systemState.prevMajorGcTime = systemState.lastMajorGcTime;
            systemState.prevUnknownGcTime = systemState.lastUnknownGcTime;
            systemState.prevTotalGcTime = systemState.lastTotalGcTime;
            systemState.lastUpdateTime = updateTime;
            systemState.lastMinorGcTime = minorGcTime;
            systemState.lastMajorGcTime = majorGcTime;
            systemState.lastUnknownGcTime = unknownGcTime;
            systemState.lastTotalGcTime = minorGcTime + majorGcTime + unknownGcTime;

            updateSystemState();
        }

    }

    private class SystemState {

        private volatile long prevUpdateTime;
        private volatile long prevMinorGcTime;
        private volatile long prevMajorGcTime;
        private volatile long prevUnknownGcTime;
        private volatile long prevTotalGcTime;
        private volatile long lastUpdateTime;
        private volatile long lastMinorGcTime;
        private volatile long lastMajorGcTime;
        private volatile long lastUnknownGcTime;
        private volatile long lastTotalGcTime;
        private volatile boolean ok = true;

        @Override
        public String toString() {
            return "SystemState{"
                     + "prevUpdateTime=" + prevUpdateTime
                     + ", prevMinorGcTime=" + prevMinorGcTime
                     + ", prevMajorGcTime=" + prevMajorGcTime
                     + ", prevUnknownGcTime=" + prevUnknownGcTime
                     + ", prevTotalGcTime=" + prevTotalGcTime
                     + ", lastUpdateTime=" + lastUpdateTime
                     + ", lastMinorGcTime=" + lastMinorGcTime
                     + ", lastMajorGcTime=" + lastMajorGcTime
                     + ", lastUnknownGcTime=" + lastUnknownGcTime
                     + ", lastTotalGcTime=" + lastTotalGcTime
                     + ", ok=" + ok
                     + '}';
        }

    }

}
