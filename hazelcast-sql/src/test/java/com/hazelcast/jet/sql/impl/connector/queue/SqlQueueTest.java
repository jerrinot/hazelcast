package com.hazelcast.jet.sql.impl.connector.queue;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlResult;
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
        sqlService.execute("CREATE MAPPING my_queue(\n"
                + "    id INT,\n"
                + "    name VARCHAR\n"
                + ")\n"
                + "TYPE Queue\n"
                + "OPTIONS (\n"
                + "    'valueFormat' = 'json'\n"
                + ")");
        SqlResult execute = sqlService.execute("select * from my_queue");
        System.out.println(execute);
    }
}
