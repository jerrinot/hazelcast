/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static com.hazelcast.query.Predicates.and;
import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.in;
import static com.hazelcast.query.impl.predicates.PredicateAssertions.assertContainsEqualsPredicate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RuleBasedQueryOptimizerTest {

    private RuleBasedQueryOptimizer optimizer;
    private Indexes mockIndexes;


    @Before
    public void setUp() {
        mockIndexes = createMockIndexes();
        optimizer = new RuleBasedQueryOptimizer();
    }

    @Test
    public void testAndFlattening_whenHasMoreThanOneInnerAndPredicate_thenFlattenItAll() {
        //(a1 = 1 and (a2 = 2 and (a3 = 3 and a4 = 4))  -->  (a1 = 1 and a2 = 2 and a3 = 3 and a4 = 4)

        Predicate a1 = equal("a1", 1);
        Predicate a2 = equal("a2", 2);
        Predicate a3 = equal("a3", 3);
        Predicate a4 = equal("a4", 4);

        AndPredicate innerInnerAnd = (AndPredicate) and(a3, a4);
        AndPredicate innerAnd = (AndPredicate) and(a2, innerInnerAnd);
        AndPredicate outerAnd = (AndPredicate) and(a1, innerAnd);

        AndPredicate result = (AndPredicate) optimizer.optimize(outerAnd, mockIndexes);

        Predicate[] inners = result.predicates;
        assertEquals(4, inners.length);

        assertContainsEqualsPredicate(inners, "a1", 1);
        assertContainsEqualsPredicate(inners, "a2", 2);
        assertContainsEqualsPredicate(inners, "a3", 3);
        assertContainsEqualsPredicate(inners, "a4", 4);
    }

    private Indexes createMockIndexes() {
        Indexes mockIndexes = mock(Indexes.class);
        Index mockIndex = mock(Index.class);
        when(mockIndexes.getIndex(anyString())).thenReturn(mockIndex);

        return mockIndexes;
    }
}
