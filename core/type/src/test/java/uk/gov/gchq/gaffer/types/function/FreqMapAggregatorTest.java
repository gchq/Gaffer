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
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.koryphe.binaryoperator.BinaryOperatorTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class FreqMapAggregatorTest extends BinaryOperatorTest {
    @Test
    public void shouldMergeFreqMaps() {
        // Given
        final FreqMapAggregator aggregator = new FreqMapAggregator();

        final FreqMap freqMap1 = new FreqMap();
        freqMap1.put("1", 2L);
        freqMap1.put("2", 3L);

        final FreqMap freqMap2 = new FreqMap();
        freqMap2.put("2", 4L);
        freqMap2.put("3", 5L);

        // When
        final FreqMap result = aggregator.apply(freqMap1, freqMap2);

        // Then
        assertEquals((Long) 2L, result.get("1"));
        assertEquals((Long) 7L, result.get("2"));
        assertEquals((Long) 5L, result.get("3"));
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final FreqMapAggregator aggregator = new FreqMapAggregator();

        // When 1
        final String json = new String(JSONSerialiser.serialise(aggregator, true));

        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.types.function.FreqMapAggregator\"%n" +
                "}"), json);

        // When 2
        final FreqMapAggregator deserialisedAggregator = JSONSerialiser.deserialise(json.getBytes(), getFunctionClass());

        // Then 2
        assertNotNull(deserialisedAggregator);
    }

    @Override
    protected FreqMapAggregator getInstance() {
        return new FreqMapAggregator();
    }

    @Override
    protected Class<FreqMapAggregator> getFunctionClass() {
        return FreqMapAggregator.class;
    }
}
