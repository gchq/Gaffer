/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.function;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ArrayTupleTest {

    @Test
    public void shouldConstructArrayTuple() {
        // Given
        final ArrayTuple tuple = new ArrayTuple(new String[]{"val1", "val2"});

        // When
        final Object _1 = tuple.get(0);
        final Object _2 = tuple.get(1);

        // Then
        assertEquals("val1", _1);
        assertEquals("val2", _2);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldBeImmutable() {
        // Given
        final ArrayTuple tuple = new ArrayTuple(new String[]{"val1", "val2"});

        // When
        tuple.put(0, "newVal1");
        tuple.put(1, "newVal2");
    }

    @Test
    public void shouldBeEqual() {
        // Given
        final ArrayTuple tuple = new ArrayTuple(new String[]{"val1", "val2"});
        final ArrayTuple other = new ArrayTuple(new String[]{"val1", "val2"});

        // Then
        assertEquals(tuple, other);
        assertEquals(tuple.hashCode(), other.hashCode());
    }

}
