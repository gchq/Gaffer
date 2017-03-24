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
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.koryphe.predicate.PredicateTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the MapFilter class can be used to filter {@code FreqMap} frequencies.
 */
public class FreqMapIsMoreThanTest extends PredicateTest {
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
        final PredicateMap<Long> filter = new PredicateMap<>(KEY1, new IsMoreThan(0L));

        // When
        boolean accepted = filter.test(map1);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldAcceptWhenFreqIsEqualTo1() {
        // Given
        final PredicateMap<Long> filter = new PredicateMap<>(KEY1, new IsMoreThan(1L, true));

        // When
        boolean accepted = filter.test(map1);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldRejectWhenEqualToAndFlagNotSet() {
        // Given
        final PredicateMap<Long> filter = new PredicateMap<>(KEY1, new IsMoreThan(1L, false));

        // When
        boolean accepted = filter.test(map1);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldRejectWhenLessThan10() {
        // Given
        final PredicateMap<Long> filter = new PredicateMap<>(KEY1, new IsMoreThan(10L));

        // When
        boolean accepted = filter.test(map1);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldRejectWhenKeyNotPresent() {
        // Given
        final PredicateMap<Long> filter = new PredicateMap<>(KEY2, new IsMoreThan(10L));

        // When
        boolean accepted = filter.test(map1);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldRejectEmptyMaps() {
        // Given
        final PredicateMap<Long> filter = new PredicateMap<>(KEY1, new IsMoreThan(0L));

        // When
        boolean accepted = filter.test(new FreqMap());

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final PredicateMap<Comparable> filter = new PredicateMap<>(KEY1, new IsMoreThan(1L, true));

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.filter.PredicateMap\",%n" +
                "  \"predicate\" : {%n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.function.filter.IsMoreThan\",%n" +
                "    \"orEqualTo\" : true,%n" +
                "    \"value\" : {%n" +
                "      \"java.lang.Long\" : 1%n" +
                "    }%n" +
                "  },%n" +
                "  \"key\" : \"key1\"%n" +
                "}"), json);

        // When 2
        final PredicateMap<Long> deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), PredicateMap.class);

        // Then 2
        assertNotNull(deserialisedFilter);
        assertEquals(KEY1, deserialisedFilter.getKey());
    }

    @Override
    protected Class<PredicateMap> getPredicateClass() {
        return PredicateMap.class;
    }

    @Override
    protected PredicateMap<Long> getInstance() {
        return new PredicateMap<>(KEY1, new IsMoreThan(0L));
    }
}
