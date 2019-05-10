/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.types.function;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.IntegerFreqMap;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IntegerFreqMapAggregatorTest extends BinaryOperatorTest {
    @Test
    public void shouldMergeFreqMaps() {
        // Given
        final IntegerFreqMapAggregator aggregator = new IntegerFreqMapAggregator();

        final IntegerFreqMap freqMap1 = new IntegerFreqMap();
        freqMap1.put("1", 2);
        freqMap1.put("2", 3);

        final IntegerFreqMap freqMap2 = new IntegerFreqMap();
        freqMap2.put("2", 4);
        freqMap2.put("3", 5);

        // When
        final IntegerFreqMap result = aggregator.apply(freqMap1, freqMap2);

        // Then
        assertEquals((Integer) 2, result.get("1"));
        assertEquals((Integer) 7, result.get("2"));
        assertEquals((Integer) 5, result.get("3"));
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final IntegerFreqMapAggregator aggregator = new IntegerFreqMapAggregator();

        // When 1
        final String json = new String(JSONSerialiser.serialise(aggregator, true));

        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.types.function.IntegerFreqMapAggregator\"%n" +
                "}"), json);

        // When 2
        final IntegerFreqMapAggregator deserialisedAggregator = JSONSerialiser.deserialise(json.getBytes(), getFunctionClass());

        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected IntegerFreqMapAggregator getInstance() {
        return new IntegerFreqMapAggregator();
    }

    @Override
    protected Class<IntegerFreqMapAggregator> getFunctionClass() {
        return IntegerFreqMapAggregator.class;
    }
}
