/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Triggers map loading from a map store
 */
public class LoadKeysOperation extends AbstractMapOperation {

    private Collection<Data> keys;
    private boolean allKeysLoaded;

    public LoadKeysOperation() {
    }

    public LoadKeysOperation(String name, Collection<Data> keys, boolean allKeysLoaded) {
        super(name);
        this.keys = keys;
        this.allKeysLoaded = allKeysLoaded;
    }

    @Override
    public void run() throws Exception {
        MapStoreContext mapStoreContext = mapContainer.getMapStoreContext();
        mapStoreContext.addInitialKeys(keys, allKeysLoaded);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        final int size = keys.size();
        out.writeInt(size);
        for (Data key : keys) {
            out.writeData(key);
        }
        out.writeBoolean(allKeysLoaded);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        final int size = in.readInt();
        if (size > 0) {
            keys = new ArrayList<Data>(size);
        }
        for (int i = 0; i < size; i++) {
            Data data = in.readData();
            keys.add(data);
        }
        allKeysLoaded = in.readBoolean();
    }
}
