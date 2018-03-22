/*
 * Copyright 2017-2018 Crown Copyright
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PairTest {

    @Test
    public void shouldCreateMutablePair() throws Exception {
        // Given
        final Pair<Integer, String> pair = new Pair<>(0, "foo");
        final Pair<Object, String> pair2 = new Pair<>(null, "bar");

        // Then
        assertTrue(pair instanceof Pair<?, ?>);
        assertTrue(pair2 instanceof Pair<?, ?>);

        assertEquals(0, pair.getFirst().intValue());
        assertNull(pair2.getFirst());

        assertEquals("foo", pair.getSecond());
        assertEquals("bar", pair2.getSecond());
    }

    @Test
    public void shouldBeAbleToMutateMutablePair() {
        // Given
        final Pair<Integer, String> pair = new Pair<>(0);
        final Pair<Object, String> pair2 = new Pair<>();

        // When
        pair.setFirst(1);
        pair2.setSecond("baz");

        // Then
        assertTrue(pair instanceof Pair<?, ?>);
        assertTrue(pair2 instanceof Pair<?, ?>);

        assertEquals(1, pair.getFirst().intValue());
        assertEquals("baz", pair2.getSecond());

        assertNull(pair.getSecond());
        assertNull(pair2.getFirst());
    }
}
