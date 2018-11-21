/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapValue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

public class RemoveOperation extends AbstractBackupAwareMultiMapOperation implements MutatingOperation {

    private static final boolean HACK_ENABLED;
    private static Field MAP_FIELD;
    private static Method GET_ENTRY_METHOD;

    private Data value;
    private long recordId;

    static {
        HACK_ENABLED = enableHack();
    }

    private static boolean enableHack() {
        try {
            Field mapField = HashSet.class.getDeclaredField("map");
            if (!mapField.isAccessible()) {
                mapField.setAccessible(true);
            }
            Method getEntryMethod = HashMap.class.getDeclaredMethod("getEntry", Object.class);
            if (!getEntryMethod.isAccessible()) {
                getEntryMethod.setAccessible(true);
            }

            MAP_FIELD = mapField;
            GET_ENTRY_METHOD = getEntryMethod;
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    public RemoveOperation() {
    }

    public RemoveOperation(String name, Data dataKey, long threadId, Data value) {
        super(name, dataKey, threadId);
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        response = false;
        MultiMapContainer container = getOrCreateContainer();
        MultiMapValue multiMapValue = container.getMultiMapValueOrNull(dataKey);
        if (multiMapValue == null) {
            return;
        }
        Collection<MultiMapRecord> coll = multiMapValue.getCollection(false);
        MultiMapRecord record = new MultiMapRecord(isBinary() ? value : toObject(value));
        MultiMapRecord matchingRecord = getMatchingMultimapRecordIfExist(coll, record);
        if (matchingRecord == null) {
            Iterator<MultiMapRecord> iterator = coll.iterator();
            while (iterator.hasNext()) {
                MultiMapRecord r = iterator.next();
                if (r.equals(record)) {
                    iterator.remove();
                    recordId = r.getRecordId();
                    response = true;
                    if (coll.isEmpty()) {
                        container.delete(dataKey);
                    }
                    break;
                }
            }
        } else {
            coll.remove(record);
            recordId = matchingRecord.getRecordId();
            response = true;
            if (coll.isEmpty()) {
                container.delete(dataKey);
            }
        }
    }

    private static MultiMapRecord getMatchingMultimapRecordIfExist(Collection<MultiMapRecord> coll, MultiMapRecord r) {
        if (!HACK_ENABLED) {
            return null;
        }
        if (!(coll instanceof HashSet)) {
            return null;
        }
        HashSet<MultiMapRecord> set = (HashSet<MultiMapRecord>) coll;
        try {
            HashMap backingMap = (HashMap) MAP_FIELD.get(set);
            if (backingMap == null) {
                return null;
            }
            Map.Entry<MultiMapRecord, ?> entry = (Map.Entry<MultiMapRecord, ?>) GET_ENTRY_METHOD.invoke(backingMap, r);
            if (entry == null) {
                return null;
            }
            return entry.getKey();
        } catch (IllegalAccessException e) {
            return null;
        } catch (InvocationTargetException e) {
            return null;
        }

    }

    @Override
    public void afterRun() throws Exception {
        if (Boolean.TRUE.equals(response)) {
            getOrCreateContainer().update();
            publishEvent(EntryEventType.REMOVED, dataKey, null, value);
        }
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new RemoveBackupOperation(name, dataKey, recordId);
    }

    @Override
    public void onWaitExpire() {
        sendResponse(false);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = in.readData();
    }

    @Override
    public int getId() {
        return MultiMapDataSerializerHook.REMOVE;
    }
}
