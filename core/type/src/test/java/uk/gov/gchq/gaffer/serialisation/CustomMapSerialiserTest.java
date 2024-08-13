/*
 * Copyright 2017-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.serialisation;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedFloatSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedIntegerSerialiser;
import uk.gov.gchq.gaffer.types.CustomMap;

import static org.assertj.core.api.Assertions.assertThat;

class CustomMapSerialiserTest extends ToBytesSerialisationTest<CustomMap> {

    @Test
    void shouldSerialiseStringInt() throws SerialisationException {
        // Given
        final CustomMap<String, Integer> expected = new CustomMap<>(new StringSerialiser(), new OrderedIntegerSerialiser());
        expected.put("one", 111);
        expected.put("two", 221);

        // When
        final CustomMap deserialise = serialiser.deserialise(serialiser.serialise(expected));

        // Then
        if (deserialise.equals(expected)) {
            return;
        }
        detailedEquals(expected, deserialise, String.class, Integer.class, new StringSerialiser(), new OrderedIntegerSerialiser());
    }

    private void detailedEquals(final CustomMap<String, Integer> expected,
                                final CustomMap actual,
                                final Class<String> expectedKClass,
                                final Class<Integer> expectedVClass,
                                final ToBytesSerialiser<String> kS,
                                final ToBytesSerialiser<Integer> vS) {

        // Serialiser
        assertThat(expected.getKeySerialiser()).isEqualTo(kS);
        assertThat(actual.getKeySerialiser()).isEqualTo(kS);
        assertThat(expected.getValueSerialiser()).isEqualTo(vS);
        assertThat(actual.getValueSerialiser()).isEqualTo(vS);
        assertThat(actual.getKeySerialiser()).isEqualTo(expected.getKeySerialiser());

        // Key element
        assertThat(expected.keySet().iterator().next().getClass()).isEqualTo(expectedKClass);
        assertThat(actual.keySet().iterator().next().getClass()).isEqualTo(expectedKClass);

        // Value element
        assertThat(expected.values().iterator().next().getClass()).isEqualTo(expectedVClass);
        assertThat(actual.values().iterator().next().getClass()).isEqualTo(expectedVClass);

        // keySets
        assertThat(actual.keySet()).isEqualTo(expected.keySet());

        //values
        for (Object k : expected.keySet()) {
            final Object expectedV = expected.get(k);
            final Object actualV = actual.get(k);
            assertThat(actualV.getClass()).isEqualTo(expectedV.getClass());
            assertThat(actualV.getClass()).isEqualTo(expectedVClass);
            assertThat(expectedV.getClass()).isEqualTo(expectedVClass);
            assertThat(actualV).isEqualTo(expectedV);
        }
        assertThat(actual).isEqualTo(expected);
    }

    @Override
    public Serialiser<CustomMap, byte[]> getSerialisation() {
        return new CustomMapSerialiser();
    }

    @Override
    public Pair<CustomMap, byte[]>[] getHistoricSerialisationPairs() {
        final CustomMap<String, Integer> cm1 = new CustomMap<>(new StringSerialiser(), new OrderedIntegerSerialiser());
        cm1.put("One", 1);

        final CustomMap<Float, String> cm2 = new CustomMap<>(new OrderedFloatSerialiser(), new StringSerialiser());
        cm2.put(3.1f, "three point 1");

        return new Pair[] {
                new Pair(cm1, new byte[] {-84, -19, 0, 5, 115, 114, 0, 69, 117, 107, 46, 103, 111, 118, 46, 103, 99, 104, 113, 46, 103, 97, 102, 102, 101, 114, 46, 115, 101, 114, 105, 97, 108, 105, 115, 97, 116, 105, 111, 110, 46, 67, 117, 115, 116, 111, 109, 77, 97, 112, 83, 101, 114, 105, 97, 108, 105, 115, 101, 114, 36, 67, 117, 115, 116, 111, 109, 77, 97, 112, 73, 110, 116, 101, 114, 105, 109, 112, 50, 94, -101, 109, -71, 126, 20, 2, 0, 3, 91, 0, 7, 98, 121, 116, 101, 77, 97, 112, 116, 0, 2, 91, 66, 76, 0, 13, 107, 101, 121, 83, 101, 114, 105, 97, 108, 105, 115, 101, 114, 116, 0, 52, 76, 117, 107, 47, 103, 111, 118, 47, 103, 99, 104, 113, 47, 103, 97, 102, 102, 101, 114, 47, 115, 101, 114, 105, 97, 108, 105, 115, 97, 116, 105, 111, 110, 47, 84, 111, 66, 121, 116, 101, 115, 83, 101, 114, 105, 97, 108, 105, 115, 101, 114, 59, 76, 0, 15, 118, 97, 108, 117, 101, 83, 101, 114, 105, 97, 108, 105, 115, 101, 114, 113, 0, 126, 0, 2, 120, 112, 117, 114, 0, 2, 91, 66, -84, -13, 23, -8, 6, 8, 84, -32, 2, 0, 0, 120, 112, 0, 0, 0, 10, 3, 79, 110, 101, 5, 4, -128, 0, 0, 1, 115, 114, 0, 64, 117, 107, 46, 103, 111, 118, 46, 103, 99, 104, 113, 46, 103, 97, 102, 102, 101, 114, 46, 115, 101, 114, 105, 97, 108, 105, 115, 97, 116, 105, 111, 110, 46, 105, 109, 112, 108, 101, 109, 101, 110, 116, 97, 116, 105, 111, 110, 46, 83, 116, 114, 105, 110, 103, 83, 101, 114, 105, 97, 108, 105, 115, 101, 114, 78, 96, -36, -19, 29, -23, 32, -19, 2, 0, 0, 120, 114, 0, 61, 117, 107, 46, 103, 111, 118, 46, 103, 99, 104, 113, 46, 103, 97, 102, 102, 101, 114, 46, 115, 101, 114, 105, 97, 108, 105, 115, 97, 116, 105, 111, 110, 46, 84, 111, 66, 121, 116, 101, 115, 86, 105, 97, 83, 116, 114, 105, 110, 103, 68, 101, 115, 101, 114, 105, 97, 108, 105, 115, 101, 114, -88, 123, -44, -50, -55, 101, -92, 46, 2, 0, 1, 76, 0, 7, 99, 104, 97, 114, 115, 101, 116, 116, 0, 18, 76, 106, 97, 118, 97, 47, 108, 97, 110, 103, 47, 83, 116, 114, 105, 110, 103, 59, 120, 112, 116, 0, 5, 85, 84, 70, 45, 56, 115, 114, 0, 80, 117, 107, 46, 103, 111, 118, 46, 103, 99, 104, 113, 46, 103, 97, 102, 102, 101, 114, 46, 115, 101, 114, 105, 97, 108, 105, 115, 97, 116, 105, 111, 110, 46, 105, 109, 112, 108, 101, 109, 101, 110, 116, 97, 116, 105, 111, 110, 46, 111, 114, 100, 101, 114, 101, 100, 46, 79, 114, 100, 101, 114, 101, 100, 73, 110, 116, 101, 103, 101, 114, 83, 101, 114, 105, 97, 108, 105, 115, 101, 114, 78, -75, -61, 54, -72, 114, -23, -42, 2, 0, 0, 120, 112}),
                new Pair(cm2, new byte[] {-84, -19, 0, 5, 115, 114, 0, 69, 117, 107, 46, 103, 111, 118, 46, 103, 99, 104, 113, 46, 103, 97, 102, 102, 101, 114, 46, 115, 101, 114, 105, 97, 108, 105, 115, 97, 116, 105, 111, 110, 46, 67, 117, 115, 116, 111, 109, 77, 97, 112, 83, 101, 114, 105, 97, 108, 105, 115, 101, 114, 36, 67, 117, 115, 116, 111, 109, 77, 97, 112, 73, 110, 116, 101, 114, 105, 109, 112, 50, 94, -101, 109, -71, 126, 20, 2, 0, 3, 91, 0, 7, 98, 121, 116, 101, 77, 97, 112, 116, 0, 2, 91, 66, 76, 0, 13, 107, 101, 121, 83, 101, 114, 105, 97, 108, 105, 115, 101, 114, 116, 0, 52, 76, 117, 107, 47, 103, 111, 118, 47, 103, 99, 104, 113, 47, 103, 97, 102, 102, 101, 114, 47, 115, 101, 114, 105, 97, 108, 105, 115, 97, 116, 105, 111, 110, 47, 84, 111, 66, 121, 116, 101, 115, 83, 101, 114, 105, 97, 108, 105, 115, 101, 114, 59, 76, 0, 15, 118, 97, 108, 117, 101, 83, 101, 114, 105, 97, 108, 105, 115, 101, 114, 113, 0, 126, 0, 2, 120, 112, 117, 114, 0, 2, 91, 66, -84, -13, 23, -8, 6, 8, 84, -32, 2, 0, 0, 120, 112, 0, 0, 0, 20, 5, 4, 64, 70, 102, 102, 13, 116, 104, 114, 101, 101, 32, 112, 111, 105, 110, 116, 32, 49, 115, 114, 0, 78, 117, 107, 46, 103, 111, 118, 46, 103, 99, 104, 113, 46, 103, 97, 102, 102, 101, 114, 46, 115, 101, 114, 105, 97, 108, 105, 115, 97, 116, 105, 111, 110, 46, 105, 109, 112, 108, 101, 109, 101, 110, 116, 97, 116, 105, 111, 110, 46, 111, 114, 100, 101, 114, 101, 100, 46, 79, 114, 100, 101, 114, 101, 100, 70, 108, 111, 97, 116, 83, 101, 114, 105, 97, 108, 105, 115, 101, 114, 94, -57, -120, -117, -127, 8, 64, 109, 2, 0, 0, 120, 112, 115, 114, 0, 64, 117, 107, 46, 103, 111, 118, 46, 103, 99, 104, 113, 46, 103, 97, 102, 102, 101, 114, 46, 115, 101, 114, 105, 97, 108, 105, 115, 97, 116, 105, 111, 110, 46, 105, 109, 112, 108, 101, 109, 101, 110, 116, 97, 116, 105, 111, 110, 46, 83, 116, 114, 105, 110, 103, 83, 101, 114, 105, 97, 108, 105, 115, 101, 114, 78, 96, -36, -19, 29, -23, 32, -19, 2, 0, 0, 120, 114, 0, 61, 117, 107, 46, 103, 111, 118, 46, 103, 99, 104, 113, 46, 103, 97, 102, 102, 101, 114, 46, 115, 101, 114, 105, 97, 108, 105, 115, 97, 116, 105, 111, 110, 46, 84, 111, 66, 121, 116, 101, 115, 86, 105, 97, 83, 116, 114, 105, 110, 103, 68, 101, 115, 101, 114, 105, 97, 108, 105, 115, 101, 114, -88, 123, -44, -50, -55, 101, -92, 46, 2, 0, 1, 76, 0, 7, 99, 104, 97, 114, 115, 101, 116, 116, 0, 18, 76, 106, 97, 118, 97, 47, 108, 97, 110, 103, 47, 83, 116, 114, 105, 110, 103, 59, 120, 112, 116, 0, 5, 85, 84, 70, 45, 56}),
        };
    }
}
