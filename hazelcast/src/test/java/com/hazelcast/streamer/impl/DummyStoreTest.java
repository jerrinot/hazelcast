package com.hazelcast.streamer.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
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

import java.io.File;
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
        File baseDir = folder.newFolder();
        int partitionId = 0;
        int entryCount = 1000;

        DummyStore store = new DummyStore(name, partitionId, baseDir);
        for (int i = 0; i < entryCount; i++) {
            store.add(serializationService.toData(i));
        }

        PollResult pollResult = new PollResult();
        for (int i = 0; i < entryCount; i++) {
            store.read(i, 1, pollResult);
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
        File baseDir = folder.newFolder();
        int partitionId = 0;
        int entryCount = 10 * 1000 * 1000;

        DummyStore store = new DummyStore(name, partitionId, baseDir);
        for (int i = 0; i < entryCount; i++) {
            store.add(serializationService.toData(i));
            if (i % 100000 == 0) {
                System.out.println("Adding " + i);
            }
        }

        int batchSize = 2000;
        for (int i = 0; i < entryCount; i+= batchSize) {
            PollResult pollResult = new PollResult();
            store.read(i, batchSize, pollResult);

            List<JournalValue<Data>> results = pollResult.getResults();
            assertEquals(results.size(), batchSize);

            for (int nested = 0; i < 0; i++) {
                int expectedOffset = i * batchSize + nested;
                JournalValue<Data> journalValue = results.get(nested);
                long actualOffset = journalValue.getOffset();
                assertEquals(expectedOffset, actualOffset);

                Data expectedData = serializationService.toData(expectedOffset);
                assertEquals(expectedData, journalValue.getValue());
            }
        }
    }

}