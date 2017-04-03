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
import uk.gov.gchq.koryphe.predicate.IsA;
import uk.gov.gchq.koryphe.predicate.PredicateTest;
import java.util.Date;
import java.util.Map;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * Tests the MapFilter class can be used to mapFilter {@code FreqMap} frequencies.
 */
public class PredicateMapTest extends PredicateTest {
    private static final String KEY1 = "key1";
    private static final Date DATE_KEY = new Date();
    private static final String VALUE1 = "value1";
    private static final String KEY2 = "key2";
    private Map<String, String> map;
    private Predicate<String> predicate;

    @Before
    public void setup() {
        predicate = mock(Predicate.class);

        map = mock(Map.class);
        given(map.containsKey(KEY1)).willReturn(true);
        given(map.get(KEY1)).willReturn(VALUE1);
    }

    @Test
    public void shouldAcceptWhenNotPredicateGiven() {
        // Given
        final PredicateMap<String> mapFilter = new PredicateMap<>(KEY1, null);

        // When
        boolean accepted = mapFilter.test(map);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldAcceptWhenPredicateAccepts() {
        // Given
        final PredicateMap<String> mapFilter = new PredicateMap<>(KEY1, predicate);
        given(predicate.test(VALUE1)).willReturn(true);

        // When
        boolean accepted = mapFilter.test(map);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldRejectWhenPredicateRejects() {
        // Given
        final PredicateMap<String> mapFilter = new PredicateMap<>(KEY1, predicate);
        given(predicate.test(VALUE1)).willReturn(false);

        // When
        boolean accepted = mapFilter.test(map);

        // Then
        assertFalse(accepted);
    }


    @Test
    public void shouldNotErrorWhenKeyIsNotPresentForPredicate() {
        // Given
        final PredicateMap<String> mapFilter = new PredicateMap<>(KEY2, predicate);
        given(predicate.test(null)).willReturn(false);

        // When
        boolean accepted = mapFilter.test(map);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldRejectNullMaps() {
        // Given
        final PredicateMap<String> mapFilter = new PredicateMap<>(KEY1, predicate);

        // When
        boolean accepted = mapFilter.test(null);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final PredicateMap<String> mapFilter = new PredicateMap<>(DATE_KEY, new IsA(Map.class));

        // When
        final String json = new String(new JSONSerialiser().serialise(mapFilter, true));

        // Then
        JsonUtil.assertEquals(String.format("{\n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.filter.PredicateMap\",\n" +
                "  \"predicate\" : {\n" +
                "    \"class\" : \"uk.gov.gchq.koryphe.predicate.IsA\",\n" +
                "    \"type\" : \"java.util.Map\"\n" +
                "  },\n" +
                "  \"key\" : {\n" +
                "    \"java.util.Date\" : " + DATE_KEY.getTime() + "%n" +
                "  }\n" +
                "}"), json);

        // When 2
        final PredicateMap deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), PredicateMap.class);

        // Then 2
        assertNotNull(deserialisedFilter);
        assertEquals(DATE_KEY, deserialisedFilter.getKey());
    }

    @Override
    protected Class<PredicateMap> getPredicateClass() {
        return PredicateMap.class;
    }

    @Override
    protected PredicateMap getInstance() {
        return new PredicateMap<>(KEY1, new IsA(Map.class));
    }
}
