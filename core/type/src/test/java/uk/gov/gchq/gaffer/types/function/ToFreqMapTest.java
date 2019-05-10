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
import uk.gov.gchq.koryphe.function.FunctionTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ToFreqMapTest extends FunctionTest {
    @Test
    public void shouldConvertStringToFreqMap() {
        // Given
        final ToFreqMap function = new ToFreqMap();

        final Object value = "value1";

        // When
        final FreqMap result = function.apply(value);

        // Then
        assertEquals(new FreqMap(value.toString()), result);
    }

    @Test
    public void shouldConvertObjectToFreqMap() {
        // Given
        final ToFreqMap function = new ToFreqMap();

        final Object value = 1L;

        // When
        final FreqMap result = function.apply(value);

        // Then
        assertEquals(new FreqMap(value.toString()), result);
    }

    @Test
    public void shouldConvertNullToFreqMap() {
        // Given
        final ToFreqMap function = new ToFreqMap();

        final Object value = null;

        // When
        final FreqMap result = function.apply(value);

        // Then
        assertEquals(new FreqMap((String) null), result);
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final ToFreqMap function = new ToFreqMap();

        // When 1
        final String json = new String(JSONSerialiser.serialise(function, true));

        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.types.function.ToFreqMap\"%n" +
                "}"), json);

        // When 2
        final ToFreqMap deserialisedFunction = JSONSerialiser.deserialise(json.getBytes(), getFunctionClass());

        // Then 2
        assertNotNull(deserialisedFunction);
    }

    @Override
    protected ToFreqMap getInstance() {
        return new ToFreqMap();
    }

    @Override
    protected Class<ToFreqMap> getFunctionClass() {
        return ToFreqMap.class;
    }
}
