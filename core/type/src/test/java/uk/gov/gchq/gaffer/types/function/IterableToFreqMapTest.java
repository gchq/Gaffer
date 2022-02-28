/*
 * Copyright 2021 Crown Copyright
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

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IterableToFreqMapTest extends FunctionTest<IterableToFreqMap> {

    @Test
    public void shouldInitialiseTheValueOfTheKeyToOneIfNotSeenBefore() {
        // Given
        Iterable<String> strings = (Iterable<String>) Arrays.asList("one");
        final IterableToFreqMap iterableToFreqMap =
                new IterableToFreqMap();

        // When
        FreqMap result =  iterableToFreqMap.apply(strings);

        // Then
        FreqMap expected = new FreqMap("one");
        assertEquals(expected, result);
    }

    @Test
    public void shouldIncrementTheValueOfTheKeyByOne() {
        // Given
        Iterable<String> strings = (Iterable<String>) Arrays.asList("one", "one");
        final IterableToFreqMap iterableToFreqMap =
                new IterableToFreqMap();

        // When
        FreqMap result =  iterableToFreqMap.apply(strings);

        // Then
        HashMap<String, Long> input = new HashMap<>();
        input.put("one", 2L);
        FreqMap expected = new FreqMap(input);
        assertEquals(expected, result);
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Iterable.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{FreqMap.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final IterableToFreqMap iterableToFreqMap = new IterableToFreqMap();
        // When
        final String json = new String(JSONSerialiser.serialise(iterableToFreqMap));
        IterableToFreqMap deserialisedIterableToFreqMap = JSONSerialiser.deserialise(json, IterableToFreqMap.class);
        // Then
        assertEquals(iterableToFreqMap, deserialisedIterableToFreqMap);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.types.function.IterableToFreqMap\"}", json);
    }

    @Override
    protected IterableToFreqMap getInstance() {
        return new IterableToFreqMap();
    }

    @Override
    protected Iterable<IterableToFreqMap> getDifferentInstancesOrNull() {
        return null;
    }
}
