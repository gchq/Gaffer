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
package uk.gov.gchq.gaffer.function;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import java.util.Date;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * Tests the MapFilter class can be used to mapFilter {@code FreqMap} frequencies.
 */
public class MapFilterTest extends FilterFunctionTest {
    private static final String KEY1 = "key1";
    private static final Date DATE_KEY = new Date();
    private static final String VALUE1 = "value1";
    private static final String KEY2 = "key2";
    private Map map;
    private FilterFunction filterFunction;
    private SimpleFilterFunction simpleFilterFunction;

    @Before
    public void setup() {
        filterFunction = mock(FilterFunction.class);
        simpleFilterFunction = mock(SimpleFilterFunction.class);

        map = mock(Map.class);
        given(map.containsKey(KEY1)).willReturn(true);
        given(map.get(KEY1)).willReturn(VALUE1);
    }

    @Test
    public void shouldAcceptWhenNotFilterFunctionGiven() {
        // Given
        final MapFilter mapFilter = new MapFilter(KEY1, null);

        // When
        boolean accepted = mapFilter.isValid(new Object[]{map});

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldAcceptWhenFilterFunctionAccepts() {
        // Given
        final MapFilter mapFilter = new MapFilter(KEY1, filterFunction);
        final ArgumentCaptor<Object[]> tupleCaptor = ArgumentCaptor.forClass(Object[].class);
        given(filterFunction.isValid(tupleCaptor.capture())).willReturn(true);

        // When
        boolean accepted = mapFilter.isValid(new Object[]{map});

        // Then
        assertTrue(accepted);
        assertArrayEquals(tupleCaptor.getValue(), new Object[]{VALUE1});
    }

    @Test
    public void shouldRejectWhenFilterFunctionRejects() {
        // Given
        final MapFilter mapFilter = new MapFilter(KEY1, filterFunction);
        final ArgumentCaptor<Object[]> tupleCaptor = ArgumentCaptor.forClass(Object[].class);
        given(filterFunction.isValid(tupleCaptor.capture())).willReturn(false);

        // When
        boolean accepted = mapFilter.isValid(new Object[]{map});

        // Then
        assertFalse(accepted);
        assertArrayEquals(tupleCaptor.getValue(), new Object[]{VALUE1});
    }

    @Test
    public void shouldAcceptWhenSimpleFilterFunctionAccepts() {
        // Given
        final MapFilter mapFilter = new MapFilter(KEY1, simpleFilterFunction);
        given(simpleFilterFunction.isValid(VALUE1)).willReturn(true);

        // When
        boolean accepted = mapFilter.isValid(new Object[]{map});

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldRejectWhenSimpleFilterFunctionRejects() {
        // Given
        final MapFilter mapFilter = new MapFilter(KEY1, simpleFilterFunction);
        given(simpleFilterFunction.isValid(VALUE1)).willReturn(false);

        // When
        boolean accepted = mapFilter.isValid(new Object[]{map});

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldNotErrorWhenKeyIsNotPresentForFilterFunction() {
        // Given
        final MapFilter mapFilter = new MapFilter(KEY2, filterFunction);
        final ArgumentCaptor<Object[]> tupleCaptor = ArgumentCaptor.forClass(Object[].class);
        given(filterFunction.isValid(tupleCaptor.capture())).willReturn(false);

        // When
        boolean accepted = mapFilter.isValid(new Object[]{map});

        // Then
        assertFalse(accepted);
        assertArrayEquals(tupleCaptor.getValue(), new Object[]{null});
    }

    @Test
    public void shouldNotErrorWhenKeyIsNotPresentForSimpleFilterFunction() {
        // Given
        final MapFilter mapFilter = new MapFilter(KEY2, simpleFilterFunction);
        given(simpleFilterFunction.isValid(null)).willReturn(false);

        // When
        boolean accepted = mapFilter.isValid(new Object[]{map});

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldRejectNullMaps() {
        // Given
        final MapFilter mapFilter = new MapFilter(KEY1, filterFunction);

        // When
        boolean accepted = mapFilter.isValid(new Object[]{null});

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldClone() {
        // Given
        final MapFilter mapFilter = new MapFilter(KEY1, new IsA(Map.class));

        // When
        final MapFilter clonedFilter = mapFilter.statelessClone();

        // Then
        assertNotSame(mapFilter, clonedFilter);
        assertEquals(KEY1, clonedFilter.getKey());
        assertEquals(Map.class.getName(), ((IsA) clonedFilter.getFunction()).getType());
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final MapFilter mapFilter = new MapFilter(DATE_KEY, new IsA(Map.class));

        // When
        final String json = new String(new JSONSerialiser().serialise(mapFilter, true));

        // Then
        JsonUtil.assertEquals(String.format("{\n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.MapFilter\",\n" +
                "  \"function\" : {\n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.function.IsA\",\n" +
                "    \"type\" : \"java.util.Map\"\n" +
                "  },\n" +
                "  \"key\" : {\n" +
                "    \"java.util.Date\" : " + DATE_KEY.getTime() + "%n" +
                "  }\n" +
                "}"), json);

        // When 2
        final MapFilter deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), MapFilter.class);

        // Then 2
        assertNotNull(deserialisedFilter);
        assertEquals(DATE_KEY, deserialisedFilter.getKey());
    }

    @Override
    protected Class<MapFilter> getFunctionClass() {
        return MapFilter.class;
    }

    @Override
    protected MapFilter getInstance() {
        return new MapFilter(KEY1, new IsA(Map.class));
    }
}
