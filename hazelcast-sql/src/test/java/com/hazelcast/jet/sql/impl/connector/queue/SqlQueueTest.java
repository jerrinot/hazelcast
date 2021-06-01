package com.hazelcast.jet.sql.impl.connector.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.Serializable;

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
                ClassDefinition cd = new ClassDefinitionBuilder(1, 1)
                        .addLongField("id")
                        .addStringField("name")
                        .build();

                for (long l = 0;;l++) {
//                    my_queue.offer(new HazelcastJsonValue("{\n"
//                            + "\t\"id\": "+ l +",\n"
//                            + "\t\"name\": \"joe " + l +"\"\n"
//                            + "}"));
                    GenericRecordBuilder portableBuilder = GenericRecordBuilder.portable(cd);
                    portableBuilder.setLong("id", l);
                    portableBuilder.setString("name", "name" + l);
                    my_queue.offer(portableBuilder.build());
//                    my_queue.offer(new Person(l, "name" + l));
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();

        sqlService.execute("CREATE MAPPING my_queue(\n"
                + "    id BIGINT,\n"
                + "    name VARCHAR\n"
                + ")\n"
                + "TYPE Queue\n");
        SqlResult results = sqlService.execute("select * from my_queue");
        for (SqlRow row : results) {
            System.out.println(row);
        }
    }

    public static class Person implements Serializable {
        private String name;
        private long id;

        public Person(long id, String name) {
            this.id = id;
            this.name = name;
        }
    }
}
