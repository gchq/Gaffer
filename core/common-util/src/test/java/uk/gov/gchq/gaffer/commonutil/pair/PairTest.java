/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.commonutil.pair;

import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PairTest {

    @Test
    public void shouldCreateImmutablePair() throws Exception {
        // Given
        final Pair<Integer, String> pair = new ImmutablePair<>(0, "foo");
        final Pair<Object, String> pair2 = new ImmutablePair<>(null, "bar");

        // Then
        assertTrue(pair instanceof ImmutablePair<?, ?>);
        assertTrue(pair2 instanceof ImmutablePair<?, ?>);

        assertEquals(0, pair.getFirst().intValue());
        assertNull(pair2.getFirst());

        assertEquals("foo", pair.getSecond());
        assertEquals("bar", pair2.getSecond());
    }

    @Test
    public void differentPairTypesWithSameContentsShouldBeEqual() throws Exception {
        // When
        final ImmutablePair<Integer, String> pair = new ImmutablePair<>(0, "foo");
        final MutablePair<Integer, String> pair2 = new MutablePair<>(0, "foo");
        final HashSet<Pair<Integer, String>> set = new HashSet<>();

        assertEquals(pair, pair2);
        assertEquals(pair.hashCode(), pair2.hashCode());

        // Given
        set.add(pair);
        pair2.setSecond("bar");

        // Then
        assertTrue(set.contains(pair));
        assertFalse(pair.equals(pair2));
        assertFalse(pair.hashCode() == pair2.hashCode());
    }

    @Test
    public void shouldCreateImmutablePairObjectViaStaticFactoryMethod() {
        // Given
        final Pair<String, String> pair = Pair.of("foo", "bar");

        // Then
        assertTrue(pair instanceof ImmutablePair<?, ?>);

        assertEquals("foo", pair.getFirst());
        assertEquals("bar", pair.getSecond());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldGetExceptionIfFirstArgumentIsNullInStaticFactoryMethod() {
        // Given
        final Pair<String, String> pair = Pair.of(null, "bar");
    }
}
