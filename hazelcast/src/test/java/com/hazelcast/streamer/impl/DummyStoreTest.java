package com.hazelcast.streamer.impl;

import com.hazelcast.config.StreamerConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.streamer.JournalValue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DummyStoreTest extends HazelcastTestSupport {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private InternalSerializationService serializationService;

    @Before
    public void setUp() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Before
    public void tearDown() {
        serializationService.dispose();
    }

    @Test
    public void smoke_pollSingleRecord() throws Exception {
        String name = randomName();
        StreamerConfig streamerConfig = new StreamerConfig().setName(name).setOverflowDir(folder.newFolder().getAbsolutePath());
        int partitionId = 0;
        int entryCount = 10000;

        DummyStore store = new DummyStore(name, partitionId, 271, streamerConfig);
        for (int i = 0; i < entryCount; i++) {
            store.add(serializationService.toData(i));
        }

        PollResult pollResult = new PollResult();
        long nextOffset = 0;
        for (int i = 0; i < entryCount; i++) {
            store.read(nextOffset, 1, pollResult);
            nextOffset = pollResult.getNextSequence();
        }
        List<JournalValue<Data>> results = pollResult.getResults();
        assertEquals(entryCount, results.size());

        for (int i = 0; i < entryCount; i++) {
            JournalValue<Data> journalValue = results.get(i);
            assertEquals(serializationService.toData(i), journalValue.getValue());
        }
    }

    @Test
    public void smoke_withOverflow() throws Exception {
        String name = randomName();
        StreamerConfig streamerConfig = new StreamerConfig()
                .setName(name)
                .setOverflowDir(folder.newFolder().getAbsolutePath())
                .setMaxSizeInMemoryMB(1);

        int partitionId = 0;
        int entryCount = 100000;

        DummyStore store = new DummyStore(name, partitionId, 1, streamerConfig);
        for (int i = 0; i < entryCount; i++) {
            store.add(serializationService.toData(i));
        }

        BufferObjectDataOutput bodi = serializationService.createObjectDataOutput();
        store.savePayload(bodi);

        store.dispose();
        store.restorePayload(serializationService.createObjectDataInput(bodi.toByteArray()));

        PollResult pollResult = new PollResult();
        long nextOffset = 0;
        for (int i = 0; i < entryCount; i++) {
            store.read(nextOffset, 1, pollResult);
            nextOffset = pollResult.getNextSequence();
        }
        List<JournalValue<Data>> results = pollResult.getResults();
        assertEquals(entryCount, results.size());

        for (int i = 0; i < entryCount; i++) {
            JournalValue<Data> journalValue = results.get(i);
            assertEquals(serializationService.toData(i), journalValue.getValue());
        }
    }

    @Test
    public void smoke_pollMoreRecords() throws Exception {
        String name = randomName();
        StreamerConfig streamerConfig = new StreamerConfig().setName(name).setOverflowDir(folder.newFolder().getAbsolutePath());
        int partitionId = 0;
        int entryCount = 1 * 1000 * 1000;

        DummyStore store = new DummyStore(name, partitionId, 271, streamerConfig);
        for (int i = 0; i < entryCount; i++) {
            store.add(serializationService.toData(i));
            if (i % 100000 == 0) {
                System.out.println("Adding " + i);
            }
        }

        int batchSize = 2000;
        long nextOffset = 0;
        for (int i = 0; i < entryCount; i+= batchSize) {
            if (i % 10000 == 0) {
                System.out.println("Pooling, at " + i);
            }
            PollResult pollResult = new PollResult();
            store.read(nextOffset, batchSize, pollResult);
            nextOffset = pollResult.getNextSequence();

            List<JournalValue<Data>> results = pollResult.getResults();
            assertEquals(results.size(), batchSize);

            for (int nested = 0; i < 0; i++) {
                int expectedValue = i * batchSize + nested;
                JournalValue<Data> journalValue = results.get(nested);
                Data expectedData = serializationService.toData(expectedValue);
                assertEquals(expectedData, journalValue.getValue());
            }
        }
    }

}