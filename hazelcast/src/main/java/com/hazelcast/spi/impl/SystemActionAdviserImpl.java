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

    private static final int SYSTEM_STATE_CHECK_INTERVAL_IN_MSECS = 5000;
    private static final float PERCENTAGE = 100.0F;
    private static final float MAJOR_GC_STW_TIME_PERCENTAGE_THRESHOLD = 10.0F;

    private final NodeEngineImpl nodeEngine;
    private final ExecutionService executionService;
    private final NodeExtension nodeExtension;
    private ScheduledFuture systemStateCheckerScheduler;
    private final SystemState systemState = new SystemState();

    SystemActionAdviserImpl(NodeEngineImpl nodeEngine, ExecutionService executionService) {
        this.nodeEngine = nodeEngine;
        this.executionService = executionService;
        this.nodeExtension = nodeEngine.getNode().getNodeExtension();
    }

    @Override
    public boolean shouldForceSyncBackups() {
        long now = Clock.currentTimeMillis();
        // System is under high load so system state cannot be updated in time.
        // So sync backups should be forced.
        if (now - systemState.lastUpdateTime > (2 * SYSTEM_STATE_CHECK_INTERVAL_IN_MSECS + 1000)) {
            return true;
        }
        if (systemState.prevUpdateTime > 0) {
            long updateTimeDiff = systemState.lastUpdateTime - systemState.prevUpdateTime;
            long majorGcTimeDiff = systemState.lastMajorGcTime - systemState.prevMajorGcTime;
            // Major GC time takes to much of total time, so sync backups should be forced.
            if (((majorGcTimeDiff * PERCENTAGE) / updateTimeDiff) > MAJOR_GC_STW_TIME_PERCENTAGE_THRESHOLD) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void start() {
        systemStateCheckerScheduler =
                executionService.scheduleAtFixedRate(
                        "SystemActionAdviser:systemStateChecker",
                        new SystemStateChecker(),
                        SYSTEM_STATE_CHECK_INTERVAL_IN_MSECS,
                        SYSTEM_STATE_CHECK_INTERVAL_IN_MSECS,
                        TimeUnit.MILLISECONDS);
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
            systemState.prevUpdateTime = systemState.lastUpdateTime;
            systemState.prevMinorGcTime = systemState.lastMinorGcTime;
            systemState.prevMajorGcTime = systemState.lastMajorGcTime;
            systemState.lastUpdateTime = updateTime;
            systemState.lastMinorGcTime = minorGcTime;
            systemState.lastMajorGcTime = majorGcTime;
        }

    }

    private class SystemState {

        private volatile long prevUpdateTime;
        private volatile long prevMinorGcTime;
        private volatile long prevMajorGcTime;
        private volatile long lastUpdateTime;
        private volatile long lastMinorGcTime;
        private volatile long lastMajorGcTime;

        @Override
        public String toString() {
            return "SystemState{"
                     + "prevUpdateTime=" + prevUpdateTime
                     + ", prevMinorGcTime=" + prevMinorGcTime
                     + ", prevMajorGcTime=" + prevMajorGcTime
                     + ", lastUpdateTime=" + lastUpdateTime
                     + ", lastMajorGcTime=" + lastMinorGcTime
                     + ", lastMajorGcTime=" + lastMajorGcTime
                     + '}';
        }

    }

}
