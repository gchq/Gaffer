/*
 * Copyright 2018-2019 Crown Copyright
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
import uk.gov.gchq.koryphe.tuple.MapTuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class JoinFunctionTest {
    private List<Element> leftInput = Arrays.asList(getElement(1), getElement(2), getElement(3), getElement(3), getElement(4), getElement(10));
    private List<Element> rightInput = Arrays.asList(getElement(1), getElement(2), getElement(2), getElement(3), getElement(4), getElement(12));

    @Test
    public void shouldCorrectlyJoinTwoIterablesUsingLeftKey() {
        if (null == getJoinFunction()) {
            throw new RuntimeException("No JoinFunction specified by the test.");
        }

        Iterable result = getJoinFunction().join(leftInput, rightInput, new ElementMatch(), MatchKey.LEFT, false);
        List<MapTuple> expected = getExpectedLeftKeyResults();

        assertEquals(expected.size(), ((List) result).size());
        assertTupleListsEquality(expected, (List<MapTuple>) result);
    }

    @Test
    public void shouldCorrectlyJoinTwoIterablesUsingRightKey() {
        if (null == getJoinFunction()) {
            throw new RuntimeException("No JoinFunction specified by the test.");
        }

        Iterable result = getJoinFunction().join(leftInput, rightInput, new ElementMatch(), MatchKey.RIGHT, false);
        List<MapTuple> expected = getExpectedRightKeyResults();

        assertEquals(expected.size(), ((List) result).size());
        assertTupleListsEquality(expected, (List<MapTuple>) result);
    }

    @Test
    public void shouldCorrectlyJoinTwoIterablesUsingLeftKeyAndFlattenResults() {
        if (null == getJoinFunction()) {
            throw new RuntimeException("No JoinFunction specified by the test.");
        }

        Iterable result = getJoinFunction().join(leftInput, rightInput, new ElementMatch(), MatchKey.LEFT, true);
        List<MapTuple> expected = getExpectedLeftKeyResultsFlattened();

        assertEquals(expected.size(), ((List) result).size());
        assertTupleListsEquality(expected, (List<MapTuple>) result);
    }

    @Test
    public void shouldCorrectlyJoinTwoIterablesUsingRightKeyAndFlattenResults() {
        if (null == getJoinFunction()) {
            throw new RuntimeException("No JoinFunction specified by the test.");
        }

        Iterable result = getJoinFunction().join(leftInput, rightInput, new ElementMatch(), MatchKey.RIGHT, true);
        List<MapTuple> expected = getExpectedRightKeyResultsFlattened();

        assertEquals(expected.size(), ((List) result).size());
        assertTupleListsEquality(expected, (List<MapTuple>) result);
    }

    protected Element getElement(final Integer countProperty) {
        return new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("vertex")
                .property(TestPropertyNames.SET, CollectionUtil.treeSet("3"))
                .property(TestPropertyNames.COUNT, Long.parseLong(countProperty.toString()))
                .build();
    }

    protected MapTuple<String> createMapTuple(final Object left, final Object right) {
        MapTuple<String> mapTuple = new MapTuple<>();
        mapTuple.put(MatchKey.LEFT.name(), left);
        mapTuple.put(MatchKey.RIGHT.name(), right);

        return mapTuple;

    }

    protected abstract List<MapTuple> getExpectedLeftKeyResults();

    protected abstract List<MapTuple> getExpectedRightKeyResults();

    protected abstract List<MapTuple> getExpectedLeftKeyResultsFlattened();

    protected abstract List<MapTuple> getExpectedRightKeyResultsFlattened();

    protected abstract JoinFunction getJoinFunction();

    private void assertTupleListsEquality(final List<MapTuple> expected, final List<MapTuple> actual) {
        List<Map> expectedValues = new ArrayList<>();
        List<Map> actualValues = new ArrayList<>();

        expected.forEach(mapTuple -> expectedValues.add(mapTuple.getValues()));
        actual.forEach(mapTuple -> actualValues.add(mapTuple.getValues()));

        assertTrue(actualValues.containsAll(expectedValues));
    }

    /**
     * private copy of the ElementMatch class using the count property to match by.
     */
    private class ElementMatch implements Match {
        private Iterable matchCandidates;

        @Override
        public void init(final Iterable matchCandidates) {
            this.matchCandidates = matchCandidates;
        }

        @Override
        public List matching(final Object testObject) {
            List matches = new ArrayList<>();
            ElementJoinComparator elementJoinComparator = new ElementJoinComparator(TestPropertyNames.COUNT);

            for (Object entry : matchCandidates) {
                if (elementJoinComparator.test((Element) entry, (Element) testObject)) {
                    matches.add(((Element) entry).shallowClone());
                }
            }
            return matches;
        }
    }
}
