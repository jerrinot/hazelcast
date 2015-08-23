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

package com.hazelcast.util;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.query.impl.QueryResultEntry;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.newSetFromMap;

/**
 * Collection(Set) class for result of query operations.
 */
public class QueryResultSet extends AbstractSet implements IdentifiedDataSerializable {

    private Set<QueryResultEntry> entries;
    private final SerializationService serializationService;

    private IterationType iterationType;
    private boolean data;

    public QueryResultSet() {
        serializationService = null;
    }

    public QueryResultSet(SerializationService serializationService, IterationType iterationType, boolean data) {
        this.serializationService = serializationService;
        this.iterationType = iterationType;
        this.entries = newSetFromMap(new ConcurrentHashMap<QueryResultEntry, Boolean>());
        this.data = data;
    }

    @SuppressWarnings("unchecked")
    public Iterator<Map.Entry> rawIterator() {
        return new QueryResultIterator(IterationType.ENTRY);
    }

    public boolean add(QueryResultEntry entry) {
        return entries.add(entry);
    }

    public boolean remove(QueryResultEntry entry) {
        return entries.remove(entry);
    }

    public boolean contains(QueryResultEntry entry) {
        return entries.contains(entry);
    }

    @Override
    public boolean add(Object entry) {
        return entries.add((QueryResultEntry) entry);
    }

    @Override
    public Iterator iterator() {
        return new QueryResultIterator(iterationType);
    }

    @Override
    public int size() {
        return entries.size();
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.QUERY_RESULT_SET;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(data);
        out.writeUTF(iterationType.toString());
        out.writeInt(entries.size());
        for (QueryResultEntry queryResultEntry : entries) {
            out.writeObject(queryResultEntry);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        data = in.readBoolean();
        iterationType = IterationType.valueOf(in.readUTF());
        int size = in.readInt();
        entries = newSetFromMap(new ConcurrentHashMap<QueryResultEntry, Boolean>(size));
        for (int i = 0; i < size; i++) {
            entries.add((QueryResultEntry) in.readObject());
        }
    }

    @Override
    public String toString() {
        return "QueryResultSet{"
                + "entries=" + entries()
                + ", iterationType=" + iterationType
                + ", data=" + data
                + '}';

    }

    private String entries() {
        final Iterator i = iterator();
        if (!i.hasNext()) {
            return "[]";
        }

        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (;;) {
            Object e = i.next();
            sb.append(e == this ? "(this Collection)" : e);
            if (!i.hasNext()) {
                return sb.append(']').toString();
            }
            sb.append(", ");
        }
    }

    /**
     * Iterator for this set.
     */
    private final class QueryResultIterator implements Iterator {

        private final Iterator<QueryResultEntry> iterator = entries.iterator();

        private final IterationType iteratorType;

        private QueryResultIterator(IterationType iteratorType) {
            this.iteratorType = iteratorType;
        }

        public boolean hasNext() {
            return iterator.hasNext();
        }

        @SuppressWarnings("unchecked")
        public Object next() {
            QueryResultEntry entry = iterator.next();
            if (iteratorType == IterationType.VALUE) {
                Data valueData = entry.getValueData();
                return (data) ? valueData : serializationService.toObject(valueData);
            } else if (iteratorType == IterationType.KEY) {
                Data keyData = entry.getKeyData();
                return (data) ? keyData : serializationService.toObject(keyData);
            } else {
                Data keyData = entry.getKeyData();
                Data valueData = entry.getValueData();
                if (data) {
                    return new AbstractMap.SimpleImmutableEntry(keyData, valueData);
                } else {
                    Object key = serializationService.toObject(keyData);
                    Object value = serializationService.toObject(valueData);
                    return new AbstractMap.SimpleImmutableEntry(key, value);
                }
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
