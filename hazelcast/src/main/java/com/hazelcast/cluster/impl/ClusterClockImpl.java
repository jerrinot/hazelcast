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

package com.hazelcast.cluster.impl;

import com.hazelcast.cluster.ClusterClock;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.util.Clock;

public class ClusterClockImpl implements ClusterClock {

    private final ILogger logger;

    @Probe(name = "clusterTimeDiff")
    private volatile long clusterTimeDiff = Long.MAX_VALUE;

    public ClusterClockImpl(ILogger logger) {
        this.logger = logger;
    }

    @Probe(name = "clusterTime")
    @Override
    public long getClusterTime() {
        return Clock.currentTimeMillis() + ((clusterTimeDiff == Long.MAX_VALUE) ? 0 : clusterTimeDiff);
    }

    public void setMasterTime(long masterTime) {
        long diff = masterTime - Clock.currentTimeMillis();
        if (logger.isFinestEnabled()) {
            logger.finest("Setting cluster time diff to " + diff + "ms.");
        }
        this.clusterTimeDiff = diff;
    }

    @Override
    public long getClusterTimeDiff() {
        return (clusterTimeDiff == Long.MAX_VALUE) ? 0 : clusterTimeDiff;
    }
}
