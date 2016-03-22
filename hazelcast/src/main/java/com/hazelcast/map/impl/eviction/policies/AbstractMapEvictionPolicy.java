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

package com.hazelcast.map.impl.eviction.policies;

import com.hazelcast.core.EntryView;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Contains common methods to implement {@link MapEvictionPolicy}
 */
public abstract class AbstractMapEvictionPolicy implements MapEvictionPolicy {

    /**
     * Default sample count to collect
     */
    private static final int DEFAULT_SAMPLE_COUNT = Integer.getInteger("hazelcast.eviction.sample.count", 15);

    @Override
    public EntryView selectEvictableEntry(Iterable<EntryView> samples, EntryView[] evictionPool) {
        List<EntryView> merged = new ArrayList<EntryView>(evictionPool.length * 2);
        for (EntryView view : samples) {
            merged.add(view);
        }
        for (EntryView view : evictionPool) {
            if (view != null) {
                merged.add(view);
            }
        }

        Collections.sort(merged, new Comparator<EntryView>() {
            @Override
            public int compare(EntryView o1, EntryView o2) {
                long x = o1.getLastAccessTime();
                long y = o2.getLastAccessTime();
                return (x < y) ? -1 : ((x == y) ? 0 : 1);
            }
        });

        EntryView selected = merged.get(0);
        for (int i = 0; i < evictionPool.length; i++) {
            if (i < merged.size()-1) {
                EntryView view = merged.get(i+1);
                evictionPool[i] = view;
            }
        }

        return selected;
    }

    @Override
    public int getSampleCount() {
        return DEFAULT_SAMPLE_COUNT;
    }
}
