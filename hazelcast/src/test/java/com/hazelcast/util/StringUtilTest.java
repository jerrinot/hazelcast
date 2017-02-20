package com.hazelcast.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class StringUtilTest extends HazelcastTestSupport {

    @Test
    public void toPropertyName_whenNull_returnNull() throws Exception {
        assertEquals("", StringUtil.toPropertyName(""));
    }

    @Test
    public void toPropertyName_whenEmpty_returnEmptyString() throws Exception {
        assertEquals("", StringUtil.toPropertyName(""));
    }

    @Test
    public void toPropertyName_whenGet_returnUnchanged() throws Exception {
        assertEquals("get", StringUtil.toPropertyName("get"));
    }

    @Test
    public void toPropertyName_whenGetFoo_returnFoo() throws Exception {
        assertEquals("foo", StringUtil.toPropertyName("getFoo"));
    }

    @Test
    public void toPropertyName_whenGetF_returnF() throws Exception {
        assertEquals("f", StringUtil.toPropertyName("getF"));
    }

    @Test
    public void toPropertyName_whenGetNumber_returnNumber() throws Exception {
        assertEquals("8", StringUtil.toPropertyName("get8"));
    }

    @Test
    public void toPropertyName_whenPropertyIsLowerCase_DoNotChange() throws Exception {
        assertEquals("getfoo", StringUtil.toPropertyName("getfoo"));
    }

}