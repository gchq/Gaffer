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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ImmutablePairTest {

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
    public void shouldCreateImmutablePairWithDefaultConstructor() {
        // Given
        final Pair<Integer, String> pair = new ImmutablePair<>();
        final Pair<String, String> pair2 = new ImmutablePair<>("bar");

        // Then
        assertTrue(pair instanceof ImmutablePair<?, ?>);
        assertTrue(pair2 instanceof ImmutablePair<?, ?>);

        assertNull(pair.getFirst());
        assertEquals("bar", pair2.getFirst());

        assertNull(pair.getSecond());
        assertNull(pair2.getSecond());
    }
}
