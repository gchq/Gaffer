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
package uk.gov.gchq.gaffer.serialisation.implementation;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.IntegerSerialiser;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SetSerialiserTest extends ToBytesSerialisationTest<Set<?>> {

    @Test
    public void shouldSerialiseAndDeSerialiseSet() throws SerialisationException {
        // Given
        final Set<String> exampledSet = getExampleValue();

        // When
        final byte[] b = serialiser.serialise(exampledSet);
        final Set actual = serialiser.deserialise(b);

        // Then
        assertDeserialisedObjecttEquals(exampledSet, actual);
    }

    @Test
    public void setSerialiserWithOverlappingValuesTest() throws SerialisationException {
        // Given
        final Set<Integer> set = new LinkedHashSet<>();
        set.add(1);
        set.add(3);
        set.add(2);
        set.add(7);
        set.add(3);
        set.add(11);

        ((SetSerialiser) serialiser).setObjectSerialiser(new IntegerSerialiser());
        ((SetSerialiser) serialiser).setSetClass(LinkedHashSet.class);

        // When
        final byte[] b = serialiser.serialise(set);
        final Set o = serialiser.deserialise(b);

        // Then
        assertEquals(LinkedHashSet.class, o.getClass());
        assertEquals(5, o.size());
        assertTrue(o.contains(1));
        assertTrue(o.contains(3));
        assertTrue(o.contains(2));
        assertTrue(o.contains(7));
        assertTrue(o.contains(11));
    }

    private Set<String> getExampleValue() {
        Set<String> set = new HashSet<>();
        set.add("one");
        set.add("two");
        set.add("three");
        set.add("four");
        set.add("five");
        set.add("six");
        return set;
    }

    private void assertDeserialisedObjecttEquals(Set<String> set, Set o) {
        assertEquals(HashSet.class, o.getClass());
        assertEquals(6, o.size());
        assertEquals(set, o);
        assertTrue(o.contains("one"));
        assertTrue(o.contains("two"));
        assertTrue(o.contains("three"));
        assertTrue(o.contains("four"));
        assertTrue(o.contains("five"));
        assertTrue(o.contains("six"));
    }

    @Override
    public Serialiser<Set<?>, byte[]> getSerialisation() {
        SetSerialiser serialiser = new SetSerialiser();
        serialiser.setObjectSerialiser(new StringSerialiser());
        return serialiser;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Pair<Set<?>, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[] {new Pair<>(getExampleValue(), new byte[] {3, 115, 105, 120, 4, 102, 111, 117, 114, 3, 111, 110, 101, 3, 116, 119, 111, 5, 116, 104, 114, 101, 101, 4, 102, 105, 118, 101})};
    }

    @Test
    @Override
    public void shouldDeserialiseEmpty() throws SerialisationException {
        assertEquals(new HashSet(), serialiser.deserialiseEmpty());
    }
}
