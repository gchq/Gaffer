/*
 * Copyright 2017-2021 Crown Copyright
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SetSerialiserTest extends ToBytesSerialisationTest<Set<? extends Object>> {

    @Test
    public void shouldSerialiseAndDeSerialiseSet() throws SerialisationException {

        Set<String> set = getExampleValue();

        byte[] b = serialiser.serialise(set);
        Set o = serialiser.deserialise(b);

        assertThat(o)
                .isInstanceOf(HashSet.class)
                .hasSize(6)
                .contains("one", "two", "three", "four", "five", "six")
                .isEqualTo(set);
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

    @Test
    public void setSerialiserWithOverlappingValuesTest() throws SerialisationException {

        Set<Integer> set = new LinkedHashSet<>();
        set.add(1);
        set.add(3);
        set.add(2);
        set.add(7);
        set.add(3);
        set.add(11);

        ((SetSerialiser) serialiser).setObjectSerialiser(new IntegerSerialiser());
        ((SetSerialiser) serialiser).setSetClass(LinkedHashSet.class);

        byte[] b = serialiser.serialise(set);
        Set o = serialiser.deserialise(b);

        assertThat(o)
                .isInstanceOf(LinkedHashSet.class)
                .hasSize(5)
                .containsExactly(1, 3, 2, 7, 11);
    }

    @Override
    public Serialiser<Set<? extends Object>, byte[]> getSerialisation() {
        SetSerialiser serialiser = new SetSerialiser();
        serialiser.setObjectSerialiser(new StringSerialiser());
        return serialiser;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Pair<Set<? extends Object>, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{new Pair(getExampleValue(), new byte[]{3, 115, 105, 120, 4, 102, 111, 117, 114, 3, 111, 110, 101, 3, 116, 119, 111, 5, 116, 104, 114, 101, 101, 4, 102, 105, 118, 101})};
    }

    @Test
    @Override
    public void shouldDeserialiseEmpty() throws SerialisationException {
        assertEquals(new HashSet(), serialiser.deserialiseEmpty());
    }
}
