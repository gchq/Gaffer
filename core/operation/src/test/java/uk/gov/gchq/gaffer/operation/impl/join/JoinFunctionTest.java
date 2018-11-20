/*
 * Copyright 2018 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.operation.impl.join;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.comparison.ElementJoinComparator;
import uk.gov.gchq.gaffer.operation.impl.join.match.Match;
import uk.gov.gchq.gaffer.operation.impl.join.match.MatchKey;
import uk.gov.gchq.gaffer.operation.impl.join.methods.JoinFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class JoinFunctionTest {
    private List<Element> leftInput = Arrays.asList(getElement(1), getElement(2), getElement(3), getElement(4), getElement(10));
    private List<Element> rightInput = Arrays.asList(getElement(1), getElement(2), getElement(3), getElement(4), getElement(12));

    @Test
    public void shouldCorrectlyJoinTwoIterablesUsingLeftKey() {
        if (null == getJoinFunction()) {
            throw new RuntimeException("No JoinFunction specified by the test.");
        }

        Iterable result = getJoinFunction().join(leftInput, rightInput, new ElementMatch(), MatchKey.LEFT);

        assertEquals(getExpectedLeftKeyResults().size(), ((List) result).size());
        assertTrue(((List) result).containsAll(getExpectedLeftKeyResults()));
    }

    @Test
    public void shouldCorrectlyJoinTwoIterablesUsingRightKey() {
        if (null == getJoinFunction()) {
            throw new RuntimeException("No JoinFunction specified by the test.");
        }

        Iterable result = getJoinFunction().join(leftInput, rightInput, new ElementMatch(), MatchKey.RIGHT);

        assertEquals(getExpectedRightKeyResults().size(), ((List)result).size());
        assertTrue(((List) result).containsAll(getExpectedRightKeyResults()));
    }

    protected Element getElement(final Integer countProperty) {
        return new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, Long.parseLong(countProperty.toString()))
                .build();
    }

    protected abstract List<Map<Element, List<Element>>> getExpectedLeftKeyResults();

    protected abstract List<Map<Element, List<Element>>> getExpectedRightKeyResults();

    protected abstract JoinFunction getJoinFunction();

    /**
     * private copy of the ElementMatch class using the count property to match by.
     */
    private class ElementMatch implements Match {
        @Override
        public List matching(final Object testObject, final List testList) {
            List matches = new ArrayList<>();
            ElementJoinComparator elementJoinComparator = new ElementJoinComparator(TestPropertyNames.COUNT);

            for (Object entry : testList) {
                if (elementJoinComparator.test((Element) entry, (Element) testObject)) {
                    matches.add(((Element) entry).shallowClone());
                }
            }
            return matches;
        }
    }
}
