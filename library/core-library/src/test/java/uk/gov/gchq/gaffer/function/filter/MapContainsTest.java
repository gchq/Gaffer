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
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class MapContainsTest extends FilterFunctionTest {
    private static final String KEY1 = "key1";
    private static final String KEY2 = "key2";

    private final Map<String, Integer> map1 = new HashMap<>();

    @Before
    public void setup() {
        map1.put(KEY1, 1);
    }

    @Test
    public void shouldAcceptWhenKeyInMap() {
        // Given
        final MapContains filter = new MapContains(KEY1);

        // When
        boolean accepted = filter.isValid(map1);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldRejectWhenKeyNotPresent() {
        // Given
        final MapContains filter = new MapContains(KEY2);

        // When
        boolean accepted = filter.isValid(map1);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldRejectEmptyMaps() {
        // Given
        final MapContains filter = new MapContains(KEY1);

        // When
        boolean accepted = filter.isValid(new HashMap());

        // Then
        assertFalse(accepted);
    }


    @Test
    public void shouldClone() {
        // Given
        final MapContains filter = new MapContains(KEY1);

        // When
        final MapContains clonedFilter = filter.statelessClone();

        // Then
        assertNotSame(filter, clonedFilter);
        assertEquals(KEY1, clonedFilter.getKey());
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final MapContains filter = new MapContains(KEY1);

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.filter.MapContains\",%n" +
                "  \"key\" : \"key1\"%n" +
                "}"), json);

        // When 2
        final MapContains deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), MapContains.class);

        // Then 2
        assertNotNull(deserialisedFilter);
        assertEquals(KEY1, deserialisedFilter.getKey());
    }

    @Override
    protected Class<MapContains> getFunctionClass() {
        return MapContains.class;
    }

    @Override
    protected MapContains getInstance() {
        return new MapContains(KEY1);
    }
}
