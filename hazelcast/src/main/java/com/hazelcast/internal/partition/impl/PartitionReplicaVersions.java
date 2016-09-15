/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.logging.ILogger;

import java.util.Arrays;

import static java.lang.System.arraycopy;

final class PartitionReplicaVersions {
    final int partitionId;
    // read and updated only by operation/partition threads
    final long[] versions = new long[InternalPartition.MAX_BACKUP_COUNT];
    private final ILogger logger;
    private boolean stalling;

    PartitionReplicaVersions(int partitionId, ILogger logger) {
        this.partitionId = partitionId;
        this.logger = logger;
    }

    long[] incrementAndGet(int backupCount) {
        for (int i = 0; i < backupCount; i++) {
            versions[i]++;
        }
        return versions;
    }

    long[] get() {
        return versions;
    }

    boolean isStale(long[] newVersions, int currentReplica) {
        int index = currentReplica - 1;
        long currentVersion = versions[index];
        long newVersion = newVersions[index];
        return currentVersion > newVersion;
    }

    boolean update(long[] newVersions, int currentReplica) {
        int index = currentReplica - 1;
        long currentVersion = versions[index];
        long nextVersion = newVersions[index];
        boolean valid = (currentVersion == nextVersion - 1);
        if (valid) {
            set(newVersions, currentReplica);
            currentVersion = nextVersion;
            if (stalling) {
                logger.finest("Partition " + partitionId + " replicate " + currentReplica
                        + " is marked as stalling yet there is a successful update!");
            }
        }
        boolean ok = currentVersion >= nextVersion;
        if (!ok) {
            stalling = true;
        }
        return ok;
    }

    void set(long[] newVersions, int fromReplica) {
        int fromIndex = fromReplica - 1;
        int len = newVersions.length - fromIndex;
        arraycopy(newVersions, fromIndex, versions, fromIndex, len);
        stalling = false;
    }

    void clear() {
        for (int i = 0; i < versions.length; i++) {
            versions[i] = 0;
        }
        stalling = false;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{partitionId=" + partitionId + ", versions=" + Arrays.toString(versions) + '}';
    }
}
