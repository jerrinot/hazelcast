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

package com.hazelcast.query.impl;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.collection.InflatableSet;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Store indexes rankly.
 */
public class SortedIndexStore extends BaseIndexStore {

    private final Set<QueryableEntry> recordsWithNullValue
            = Collections.newSetFromMap(new ConcurrentHashMap<QueryableEntry, Boolean>());

    private final ConcurrentSkipListMap<Comparable, Set<QueryableEntry>> recordMap
            = new ConcurrentSkipListMap<Comparable, Set<QueryableEntry>>();

    @Override
    public void newIndex(Comparable newValue, QueryableEntry record) {
        takeWriteLock();
        try {
            if (newValue instanceof IndexImpl.NullObject) {
                recordsWithNullValue.add(record);
            } else {
                Set<QueryableEntry> records = recordMap.get(newValue);
                if (records == null) {
                    records = Collections.newSetFromMap(new ConcurrentHashMap<QueryableEntry, Boolean>(1, LOAD_FACTOR, 1));
                    recordMap.put(newValue, records);
                }
                records.add(record);
            }
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public void updateIndex(Comparable oldValue, Comparable newValue, QueryableEntry entry) {
        takeWriteLock();
        try {
            removeIndex(oldValue, entry.getIndexKey());
            newIndex(newValue, entry);
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public void removeIndex(Comparable oldValue, Data indexKey) {
        QueryableEntry eq = new QueryEntry(indexKey);

        takeWriteLock();
        try {
            if (oldValue instanceof IndexImpl.NullObject) {
                recordsWithNullValue.remove(eq);
            } else {
                Set<QueryableEntry> records = recordMap.get(oldValue);
                if (records != null) {
                    records.remove(indexKey);
                    if (records.size() == 0) {
                        recordMap.remove(eq);
                    }
                }
            }
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public void clear() {
        takeWriteLock();
        try {
            recordsWithNullValue.clear();
            recordMap.clear();
        } finally {
            releaseWriteLock();
        }
    }

    @Override
    public void getSubRecordsBetween(MultiResultSet results, Comparable from, Comparable to) {
        takeReadLock();
        try {
            SortedMap<Comparable, Set<QueryableEntry>> subMap =
                    recordMap.subMap(from, true, to, true);
            Collection<Set<QueryableEntry>> values = subMap.values();
            for (Set<QueryableEntry> entry : values) {
                results.addResultSet(entry);
            }
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public void getSubRecords(MultiResultSet results, ComparisonType comparisonType, Comparable searchedValue) {
        takeReadLock();
        try {
            SortedMap<Comparable, Set<QueryableEntry>> subMap;
            switch (comparisonType) {
                case LESSER:
                    subMap = recordMap.headMap(searchedValue, false);
                    break;
                case LESSER_EQUAL:
                    subMap = recordMap.headMap(searchedValue, true);
                    break;
                case GREATER:
                    subMap = recordMap.tailMap(searchedValue, false);
                    break;
                case GREATER_EQUAL:
                    subMap = recordMap.tailMap(searchedValue, true);
                    break;
                case NOT_EQUAL:
                    // TODO There maybe more efficient way such as
                    // Make a copy of current record map and just remove searched value.
                    // So remaining records are not equal to searched value
                    for (Map.Entry<Comparable, Set<QueryableEntry>> entry : recordMap.entrySet()) {
                        if (!searchedValue.equals(entry.getKey())) {
                            results.addResultSet(entry.getValue());
                        }
                    }
                    return;
                default:
                    throw new IllegalArgumentException("Unrecognized comparisonType: " + comparisonType);
            }
            for (Set<QueryableEntry> value : subMap.values()) {
                results.addResultSet(value);
            }
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public Set<QueryableEntry> getRecordMap(Comparable value) {
        takeReadLock();
        try {
            if (value instanceof IndexImpl.NullObject) {
                return recordsWithNullValue;
            } else {
                return recordMap.get(value);
            }
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        takeReadLock();
        try {
            if (value instanceof IndexImpl.NullObject) {
                return new SingleResultSet(recordsWithNullValue);
            } else {
                return new SingleResultSet(recordMap.get(value));
            }
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public void getRecords(MultiResultSet results, Set<Comparable> values) {
        takeReadLock();
        try {
            for (Comparable value : values) {
                Set<QueryableEntry> records;
                if (value instanceof IndexImpl.NullObject) {
                    records = recordsWithNullValue;
                } else {
                    records = recordMap.get(value);
                }
                if (records != null) {
                    results.addResultSet(records);
                }
            }
        } finally {
            releaseReadLock();
        }
    }

    @Override
    public String toString() {
        return "SortedIndexStore{"
                + "recordMap=" + recordMap.size()
                + '}';
    }
}
