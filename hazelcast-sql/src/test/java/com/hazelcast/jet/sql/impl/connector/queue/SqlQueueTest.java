package com.hazelcast.jet.sql.impl.connector.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.util.Arrays.asList;

public class SqlQueueTest extends SqlTestSupport {
    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_generateSeriesAscending() {
        new Thread() {
            @Override
            public void run() {
                IQueue<Object> my_queue = instance().getHazelcastInstance().getQueue("my_queue");
                for (;;) {
                    my_queue.offer(new HazelcastJsonValue("{\n"
                            + "\t\"id\": 123,\n"
                            + "\t\"name\": \"joe\"\n"
                            + "}"));
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();

        sqlService.execute("CREATE MAPPING my_queue(\n"
                + "    id INT,\n"
                + "    name VARCHAR\n"
                + ")\n"
                + "TYPE Queue\n"
                + "OPTIONS (\n"
                + "    'valueFormat' = 'json'\n"
                + ")");
        SqlResult results = sqlService.execute("select name from my_queue");
        for (SqlRow row : results) {
            System.out.println(row);
        }
    }
}
