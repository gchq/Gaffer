/*
 * Copyright 2017-2020 Crown Copyright
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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class PairTest {

    @Test
    public void shouldCreateMutablePair() {
        final Pair<Integer, String> pair = new Pair<>(0, "foo");

        assertEquals(0, pair.getFirst().intValue());
        assertEquals("foo", pair.getSecond());
    }

    @Test
    public void shouldCreateMutablePair2() {
        final Pair<Object, String> pair = new Pair<>(null, "bar");

        assertNull(pair.getFirst());
        assertEquals("bar", pair.getSecond());
    }

    @Test
    public void shouldBeAbleToMutateFirstInPair() {
        final Pair<Integer, String> pair = new Pair<>(0);

        pair.setFirst(1);

        assertEquals(1, pair.getFirst().intValue());
        assertNull(pair.getSecond());
    }

    @Test
    public void shouldBeAbleToMutateSecondInPair() {
        final Pair<Object, String> pair = new Pair<>();

        pair.setSecond("2nd");

        assertNull(pair.getFirst());
        assertEquals("2nd", pair.getSecond());
    }
}
