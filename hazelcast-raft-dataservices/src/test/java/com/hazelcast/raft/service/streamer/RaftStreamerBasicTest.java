package com.hazelcast.raft.service.streamer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.raft.impl.service.HazelcastRaftTestSupport;
import com.hazelcast.streamer.PollResult;
import com.hazelcast.streamer.Streamer;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RandomPicker;
import com.hazelcast.util.UuidUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.raft.service.spi.RaftProxyFactory.create;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftStreamerBasicTest extends HazelcastRaftTestSupport {

    private HazelcastInstance[] instances;
    private Streamer<String> streamer;
    private String name = "id";
    private final int raftGroupSize = 3;

    @Before
    public void setup() {
        instances = createInstances();

        streamer = createStreamer(name);
    }

    @Test
    public void testRaftProxyIsCreated() {
        assertNotNull(streamer);
    }

    @Test
    public void testSendSmokey() {
        streamer.send("foo");
    }

    @Test
    public void testSendAndPoll() throws Exception {
        streamer.send("foo").get();
        PollResult<String> poll = streamer.poll(0, 0, 1, 1, 0, TimeUnit.MINUTES);

        assertEquals(1, poll.getValues().size());
        assertEquals("foo", poll.getValues().get(0).getValue());
    }

    @Test
    public void testSendAndPollWithMaxSize() {
        int totalCount = 10000;
        int maxFetchCount = 100;

        for (int i = 0; i < totalCount; i++) {
            streamer.send(UuidUtil.newUnsecureUUID().toString());
        }
        streamer.syncBarrier();

        PollResult<String> poll = streamer.poll(0, 0, 0, maxFetchCount, 0, TimeUnit.MINUTES);
        assertEquals(maxFetchCount, poll.getValues().size());
    }


    protected Streamer<String> createStreamer(String name) {
        return create(instances[RandomPicker.getInt(instances.length)], RaftStreamerService.SERVICE_NAME, name);
    }

    protected HazelcastInstance[] createInstances() {
        return newInstances(raftGroupSize + 2, raftGroupSize, 2);
    }
}
