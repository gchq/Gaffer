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
package uk.gov.gchq.gaffer.spark.operation.dataframe.converter.property.impl;

import org.junit.Test;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.property.ConversionException;
import uk.gov.gchq.gaffer.types.FreqMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FreqMapConverterTest {
    private static final FreqMapConverter FREQ_MAP_CONVERTER = new FreqMapConverter();

    @Test
    public void testConverter() throws ConversionException {
        final FreqMap freqMap = new FreqMap();
        freqMap.put("x", 10L);
        freqMap.put("y", 5L);
        freqMap.put("z", 20L);
        final scala.collection.mutable.Map<String, Long> expectedResult = scala.collection.mutable.Map$.MODULE$.empty();
        expectedResult.put("x", 10L);
        expectedResult.put("y", 5L);
        expectedResult.put("z", 20L);
        assertEquals(expectedResult, FREQ_MAP_CONVERTER.convert(freqMap));

        final FreqMap emptyFreqMap = new FreqMap();
        expectedResult.clear();
        assertEquals(expectedResult, FREQ_MAP_CONVERTER.convert(emptyFreqMap));
    }

    @Test
    public void testCanHandleFreqMap() {
        assertTrue(FREQ_MAP_CONVERTER.canHandle(FreqMap.class));
        assertFalse(FREQ_MAP_CONVERTER.canHandle(String.class));
    }
}
