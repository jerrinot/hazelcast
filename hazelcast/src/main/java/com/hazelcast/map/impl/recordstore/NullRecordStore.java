package com.hazelcast.map.impl.recordstore;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.EntryView;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.map.impl.journal.MapEventJournal;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.monitor.LocalRecordStoreStats;
import com.hazelcast.monitor.impl.LocalRecordStoreStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class NullRecordStore implements RecordStore {

    private final String name;
    private final MapContainer mapContainer;
    private final RecordFactory recordFactory;
    private final int partitionId;
    private final ILogger logger;
    private final MapEventJournal eventJournal;
    private final MapDataStore mapDataStore;

    public NullRecordStore(MapContainer mapContainer, int partitionId, ILogger logger) {
        this.mapContainer = mapContainer;
        this.name = mapContainer.getName();
        this.recordFactory = mapContainer.getRecordFactoryConstructor().createNew(null);
        this.partitionId = partitionId;
        this.logger = logger;
        this.eventJournal = mapContainer.getMapServiceContext().getEventJournal();
        this.mapDataStore = mapContainer.getMapStoreContext().getMapStoreManager().getMapDataStore(name, partitionId);
    }

    @Override
    public LocalRecordStoreStats getLocalRecordStoreStats() {
        return new LocalRecordStoreStatsImpl();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object set(Data dataKey, Object value, long ttl) {
        return addEvent(dataKey, value);
    }

    private Record addEvent(Data dataKey, Object value) {
        eventJournal.writeAddEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(), partitionId,
                dataKey, value);
        return null;
    }

    private RuntimeException unsupportedMethod(String reason) {
        throw new UnsupportedOperationException(reason + " not unsupported in map journal");
    }

    @Override
    public Object put(Data dataKey, Object dataValue, long ttl) {
        if (ttl != RecordStore.DEFAULT_TTL) {
            throw unsupportedMethod("ttl");
        }
        return addEvent(dataKey, dataValue);
    }

    @Override
    public Object putIfAbsent(Data dataKey, Object value, long ttl, Address callerAddress) {
        throw unsupportedMethod("put if absent");
    }

    @Override
    public Record putBackup(Data key, Object value) {
        return addEvent(key, value);
    }

    @Override
    public Record putBackup(Data key, Object value, long ttl, boolean putTransient) {
        if (ttl != RecordStore.DEFAULT_TTL) {
            throw unsupportedMethod("ttl");
        }
        return addEvent(key, value);
    }

    @Override
    public boolean setWithUncountedAccess(Data dataKey, Object value, long ttl) {
        throw unsupportedMethod("entry processors");
    }

    @Override
    public Object remove(Data dataKey) {
        throw unsupportedMethod("remove");
    }

    @Override
    public boolean delete(Data dataKey) {
        throw unsupportedMethod("delete");
    }

    @Override
    public boolean remove(Data dataKey, Object testValue) {
        throw unsupportedMethod("remove");
    }

    @Override
    public void removeBackup(Data dataKey) {
        throw unsupportedMethod("remove");
    }

    @Override
    public Object get(Data dataKey, boolean backup, Address callerAddress) {
        return null;
    }

    @Override
    public Data readBackupData(Data key) {
        throw unsupportedMethod("read");
    }

    @Override
    public MapEntries getAll(Set keySet, Address callerAddress) {
        return new MapEntries(0);
    }

    @Override
    public boolean existInMemory(Data key) {
        return false;
    }

    @Override
    public boolean containsKey(Data dataKey, Address callerAddress) {
        return false;
    }

    @Override
    public int getLockedEntryCount() {
        return 0;
    }

    @Override
    public Object replace(Data dataKey, Object update) {
        throw unsupportedMethod("replace");
    }

    @Override
    public boolean replace(Data dataKey, Object expect, Object update) {
        throw unsupportedMethod("remove");
    }

    @Override
    public Object putTransient(Data dataKey, Object value, long ttl) {
        throw unsupportedMethod("put transient");
    }

    @Override
    public Object putFromLoad(Data key, Object value, Address callerAddress) {
        throw unsupportedMethod("map loader");
    }

    @Override
    public Object putFromLoadBackup(Data key, Object value) {
        throw unsupportedMethod("map loader");
    }

    @Override
    public boolean merge(Data dataKey, EntryView mergingEntry, MapMergePolicy mergePolicy) {
        throw unsupportedMethod("merge");
    }

    @Override
    public Record getRecord(Data key) {
        return null;
    }

    @Override
    public void putRecord(Data key, Record record) {
        throw unsupportedMethod("heal");
    }

    @Override
    public Iterator<Record> iterator() {
        return Collections.EMPTY_LIST.iterator();
    }

    @Override
    public Iterator<Record> iterator(long now, boolean backup) {
        return Collections.EMPTY_LIST.iterator();
    }

    @Override
    public MapKeysWithCursor fetchKeys(int tableIndex, int size) {
        return new MapKeysWithCursor(Collections.EMPTY_LIST, 0);
    }

    @Override
    public MapEntriesWithCursor fetchEntries(int tableIndex, int size) {
        return new MapEntriesWithCursor(Collections.EMPTY_LIST, 0);
    }

    @Override
    public Iterator<Record> loadAwareIterator(long now, boolean backup) {
        return Collections.EMPTY_LIST.iterator();
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean txnLock(Data key, String caller, long threadId, long referenceId, long ttl, boolean blockReads) {
        return false;
    }

    @Override
    public boolean extendLock(Data key, String caller, long threadId, long ttl) {
        return false;
    }

    @Override
    public boolean localLock(Data key, String caller, long threadId, long referenceId, long ttl) {
        return false;
    }

    @Override
    public boolean lock(Data key, String caller, long threadId, long referenceId, long ttl) {
        return false;
    }

    @Override
    public boolean isLockedBy(Data key, String caller, long threadId) {
        return false;
    }

    @Override
    public boolean unlock(Data key, String caller, long threadId, long referenceId) {
        return false;
    }

    @Override
    public boolean isLocked(Data key) {
        return false;
    }

    @Override
    public boolean isTransactionallyLocked(Data key) {
        return false;
    }

    @Override
    public boolean canAcquireLock(Data key, String caller, long threadId) {
        //we have to return false otherwise put operation would think
        //the key is locked and would block
        return true;
    }

    @Override
    public String getLockOwnerInfo(Data key) {
        return null;
    }

    @Override
    public boolean containsValue(Object testValue) {
        return false;
    }

    @Override
    public Object evict(Data key, boolean backup) {
        return null;
    }

    @Override
    public int evictAll(boolean backup) {
        return 0;
    }

    @Override
    public MapContainer getMapContainer() {
        return mapContainer;
    }

    @Override
    public long softFlush() {
        return 0;
    }

    @Override
    public void clearPartition(boolean onShutdown) {

    }

    @Override
    public void reset() {

    }

    @Override
    public boolean forceUnlock(Data dataKey) {
        return false;
    }

    @Override
    public long getOwnedEntryCost() {
        return 0;
    }

    @Override
    public int clear() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public void evictExpiredEntries(int percentage, boolean backup) {

    }

    @Override
    public boolean isExpirable() {
        return false;
    }

    @Override
    public boolean isExpired(Record record, long now, boolean backup) {
        return false;
    }

    @Override
    public void doPostEvictionOperations(Record record, boolean backup) {

    }

    @Override
    public MapDataStore<Data, Object> getMapDataStore() {
        return mapDataStore;
    }

    @Override
    public InvalidationQueue<ExpiredKey> getExpiredKeys() {
        return null;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public Record getRecordOrNull(Data key) {
        return null;
    }

    @Override
    public void evictEntries(Data excludedKey) {

    }

    @Override
    public boolean shouldEvict() {
        return false;
    }

    @Override
    public Storage createStorage(RecordFactory recordFactory, InMemoryFormat memoryFormat) {
        return null;
    }

    @Override
    public Record createRecord(Object value, long ttlMillis, long now) {
        return recordFactory.newRecord(value);
    }

    @Override
    public Record loadRecordOrNull(Data key, boolean backup, Address callerAddress) {
        return null;
    }

    @Override
    public void disposeDeferredBlocks() {

    }

    @Override
    public void destroy() {

    }

    @Override
    public void destroyInternals() {

    }

    @Override
    public void init() {

    }

    @Override
    public Storage getStorage() {
        return null;
    }

    @Override
    public void startLoading() {

    }

    @Override
    public void setPreMigrationLoadedStatus(boolean loaded) {

    }

    @Override
    public boolean isKeyLoadFinished() {
        return false;
    }

    @Override
    public boolean isLoaded() {
        return false;
    }

    @Override
    public void checkIfLoaded() throws RetryableHazelcastException {

    }

    @Override
    public void loadAll(boolean replaceExistingValues) {

    }

    @Override
    public void maybeDoInitialLoad() {

    }

    @Override
    public void updateLoadStatus(boolean lastBatch, Throwable exception) {

    }

    @Override
    public boolean hasQueryCache() {
        return false;
    }

    @Override
    public void loadAllFromStore(List keys, boolean replaceExistingValues) {

    }

    @Override
    public boolean merge(SplitBrainMergeTypes.MapMergeTypes mergingEntry, SplitBrainMergePolicy mergePolicy) {
        return false;
    }

}
