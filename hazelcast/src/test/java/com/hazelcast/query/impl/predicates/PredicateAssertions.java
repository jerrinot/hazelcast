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

import java.util.Arrays;

import static org.junit.Assert.fail;

/**
 * JUnit Assertions for predicates.
 *
 */
public class PredicateAssertions {


    public static void assertContainsEqualsPredicate(Predicate[] inners, String attributeName, Comparable value) {
        for (Predicate inner : inners) {
            if (isEqualsPredicate(inner, attributeName, value)) {
                return;
            }
        }
        fail("Predicate " + attributeName + " = " + value + " not found in " + Arrays.toString(inners));
    }

    private static boolean isEqualsPredicate(Predicate predicate, String attributeName, Comparable value) {
        if (predicate instanceof EqualPredicate) {
            EqualPredicate equalPredicate = (EqualPredicate) predicate;
            if (equalPredicate.attribute.equals(attributeName) && equalPredicate.value.equals(value)) {
                return true;
            }
        }
        return false;
    }
}
