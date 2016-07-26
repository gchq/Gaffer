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
package gaffer.function.simple.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import gaffer.exception.SerialisationException;
import gaffer.function.FilterFunctionTest;
import gaffer.types.simple.FreqMap;
import gaffer.jsonserialisation.JSONSerialiser;
import org.junit.Before;
import org.junit.Test;

public class FreqMapIsMoreThanTest extends FilterFunctionTest {
    private static final String KEY1 = "key1";
    private static final String KEY2 = "key2";

    private final FreqMap map1 = new FreqMap();

    @Before
    public void setup() {
        map1.put(KEY1, 1);
    }

    @Test
    public void shouldSetDefaultFrequencyTo0() {
        // Given
        final FreqMapIsMoreThan filter = new FreqMapIsMoreThan(KEY1);

        // When
        int freq = filter.getFrequency();

        // Then
        assertEquals(0, freq);
    }

    @Test
    public void shouldAcceptWhenFreqIsGreaterThan0() {
        // Given
        final FreqMapIsMoreThan filter = new FreqMapIsMoreThan(KEY1, 0);

        // When
        boolean accepted = filter.isValid(map1);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldAcceptWhenFreqIsEqualTo1() {
        // Given
        final FreqMapIsMoreThan filter = new FreqMapIsMoreThan(KEY1, 1, true);

        // When
        boolean accepted = filter.isValid(map1);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldRejectWhenEqualToAndFlagNotSet() {
        // Given
        final FreqMapIsMoreThan filter = new FreqMapIsMoreThan(KEY1, 1, false);

        // When
        boolean accepted = filter.isValid(map1);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldRejectWhenLessThan10() {
        // Given
        final FreqMapIsMoreThan filter = new FreqMapIsMoreThan(KEY1, 10);

        // When
        boolean accepted = filter.isValid(map1);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldRejectWhenKeyNotPresent() {
        // Given
        final FreqMapIsMoreThan filter = new FreqMapIsMoreThan(KEY2);

        // When
        boolean accepted = filter.isValid(map1);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldRejectEmptyMaps() {
        // Given
        final FreqMapIsMoreThan filter = new FreqMapIsMoreThan(KEY1);

        // When
        boolean accepted = filter.isValid(new FreqMap());

        // Then
        assertFalse(accepted);
    }


    @Test
    public void shouldClone() {
        // Given
        final FreqMapIsMoreThan filter = new FreqMapIsMoreThan(KEY1, 1, true);

        // When
        final FreqMapIsMoreThan clonedFilter = filter.statelessClone();

        // Then
        assertNotSame(filter, clonedFilter);
        assertEquals(KEY1, clonedFilter.getKey());
        assertEquals(1, clonedFilter.getFrequency());
        assertTrue(clonedFilter.getOrEqualTo());

    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final FreqMapIsMoreThan filter = new FreqMapIsMoreThan(KEY1, 1, true);

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        assertEquals(String.format("{%n" +
                "  \"class\" : \"gaffer.function.simple.filter.FreqMapIsMoreThan\",%n" +
                "  \"key\" : \"key1\",%n" +
                "  \"frequency\" : 1,%n" +
                "  \"orEqualTo\" : true%n" +
                "}"), json);

        // When 2
        final FreqMapIsMoreThan deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), FreqMapIsMoreThan.class);

        // Then 2
        assertNotNull(deserialisedFilter);
        assertEquals(KEY1, deserialisedFilter.getKey());
    }

    @Override
    protected Class<FreqMapIsMoreThan> getFunctionClass() {
        return FreqMapIsMoreThan.class;
    }

    @Override
    protected FreqMapIsMoreThan getInstance() {
        return new FreqMapIsMoreThan(KEY1);
    }
}
