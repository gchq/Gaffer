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
package uk.gov.gchq.gaffer.function.filter;

import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.FilterFunctionTest;
import uk.gov.gchq.gaffer.function.MapFilter;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.FreqMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests the MapFilter class can be used to filter {@code FreqMap} frequencies.
 */
public class FreqMapIsMoreThanTest extends FilterFunctionTest {
    private static final String KEY1 = "key1";
    private static final String KEY2 = "key2";

    private final FreqMap map1 = new FreqMap();

    @Before
    public void setup() {
        map1.put(KEY1, 1L);
    }

    @Test
    public void shouldAcceptWhenFreqIsGreaterThan0() {
        // Given
        final MapFilter filter = new MapFilter(KEY1, new IsMoreThan(0L));

        // When
        boolean accepted = filter.isValid(new Object[]{map1});

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldAcceptWhenFreqIsEqualTo1() {
        // Given
        final MapFilter filter = new MapFilter(KEY1, new IsMoreThan(1L, true));

        // When
        boolean accepted = filter.isValid(new Object[]{map1});

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldRejectWhenEqualToAndFlagNotSet() {
        // Given
        final MapFilter filter = new MapFilter(KEY1, new IsMoreThan(1L, false));

        // When
        boolean accepted = filter.isValid(new Object[]{map1});

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldRejectWhenLessThan10() {
        // Given
        final MapFilter filter = new MapFilter(KEY1, new IsMoreThan(10L));

        // When
        boolean accepted = filter.isValid(new Object[]{map1});

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldRejectWhenKeyNotPresent() {
        // Given
        final MapFilter filter = new MapFilter(KEY2, new IsMoreThan(10L));

        // When
        boolean accepted = filter.isValid(new Object[]{map1});

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldRejectEmptyMaps() {
        // Given
        final MapFilter filter = new MapFilter(KEY1, new IsMoreThan(0L));

        // When
        boolean accepted = filter.isValid(new Object[]{new FreqMap()});

        // Then
        assertFalse(accepted);
    }


    @Test
    public void shouldClone() {
        // Given
        final MapFilter filter = new MapFilter(KEY1, new IsMoreThan(1L, true));

        // When
        final MapFilter clonedFilter = filter.statelessClone();

        // Then
        assertNotSame(filter, clonedFilter);
        assertEquals(KEY1, clonedFilter.getKey());
        assertEquals(1L, ((IsMoreThan) clonedFilter.getFunction()).getControlValue());
        assertTrue(((IsMoreThan) clonedFilter.getFunction()).getOrEqualTo());
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final MapFilter filter = new MapFilter(KEY1, new IsMoreThan(1L, true));

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.MapFilter\",%n" +
                "  \"function\" : {%n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.function.filter.IsMoreThan\",%n" +
                "    \"orEqualTo\" : true,%n" +
                "    \"value\" : {%n" +
                "      \"java.lang.Long\" : 1%n" +
                "    }%n" +
                "  },%n" +
                "  \"key\" : \"key1\"%n" +
                "}"), json);

        // When 2
        final MapFilter deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), MapFilter.class);

        // Then 2
        assertNotNull(deserialisedFilter);
        assertEquals(KEY1, deserialisedFilter.getKey());
    }

    @Override
    protected Class<MapFilter> getFunctionClass() {
        return MapFilter.class;
    }

    @Override
    protected MapFilter getInstance() {
        return new MapFilter(KEY1, new IsMoreThan(0L));
    }
}
