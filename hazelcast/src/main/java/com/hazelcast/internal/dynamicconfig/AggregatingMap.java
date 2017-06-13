package com.hazelcast.internal.dynamicconfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableSet;

/**
 * TODO: Assumes maps are non-overlapping
 *
 * @param <K>
 * @param <V>
 */
public final class AggregatingMap<K, V> implements Map<K, V> {
    private final Map<K, V> map1;
    private final Map<K, V> map2;

    public static <K, V> Map<K, V> aggregate(Map<K, V> map1, Map<K, V> map2) {
        return new AggregatingMap<K, V>(map1, map2);
    }

    private AggregatingMap(Map<K, V> map1, Map<K, V> map2) {
        this.map1 = map1;
        this.map2 = map2;
    }

    @Override
    public int size() {
        return map1.size() + map2.size();
    }

    @Override
    public boolean isEmpty() {
        return map1.isEmpty() && map2.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map1.containsKey(key) || map2.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map1.containsValue(value) || map2.containsValue(value);
    }

    @Override
    public V get(Object key) {
        V v = map1.get(key);
        return v == null ? map2.get(key) : v;
    }

    @Override
    public V put(K key, V value) {
        throw new UnsupportedOperationException("aggregating map is read only");
    }

    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException("aggregating map is read only");
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException("aggregating map is read only");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("aggregating map is read only");
    }

    @Override
    public Set<K> keySet() {
        HashSet<K> keys = new HashSet<K>(map1.keySet());
        keys.addAll(map2.keySet());
        return unmodifiableSet(keys);
    }

    @Override
    public Collection<V> values() {
        ArrayList<V> values = new ArrayList<V>(map1.values());
        values.addAll(map2.values());
        return unmodifiableCollection(values);
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        HashSet<Entry<K, V>> entrySet = new HashSet<Entry<K, V>>(map1.entrySet());
        entrySet.addAll(map2.entrySet());
        return unmodifiableSet(entrySet);
    }
}
