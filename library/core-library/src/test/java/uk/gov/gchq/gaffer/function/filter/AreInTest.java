/*
 * Copyright 2016-2017 Crown Copyright
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class AreInTest extends FilterFunctionTest {
    private static final String VALUE1 = "value1";
    private static final String VALUE2 = "value2";

    private final List<Object> list = new ArrayList<>();
    private final Set<Object> set = new HashSet<>();

    @Before
    public void setup() {
        list.add(VALUE1);
        list.add(VALUE2);
        set.add(VALUE1);
        set.add(VALUE2);
    }

    @Test
    public void shouldAcceptWhenValuesAndInputAreNullOrEmpty() {
        // Given
        final AreIn filter = new AreIn();

        // When
        boolean accepted = filter.isValid((Collection) null);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldAcceptWhenValuesIsEmpty() {
        // Given
        final AreIn filter = new AreIn();

        // When
        boolean accepted = filter.isValid(list);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldAcceptWhenAllValuesInList() {
        // Given
        final AreIn filter = new AreIn(VALUE1, VALUE2);

        // When
        boolean accepted = filter.isValid(list);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldAcceptWhenAllValuesInSet() {
        // Given
        final AreIn filter = new AreIn(VALUE1, VALUE2);

        // When
        boolean accepted = filter.isValid(set);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldRejectWhenNotAllValuesPresentInList() {
        // Given
        final AreIn filter = new AreIn(VALUE1);

        // When
        boolean accepted = filter.isValid(list);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldRejectWhenNotAllValuesPresentInSet() {
        // Given
        final AreIn filter = new AreIn(VALUE1);

        // When
        boolean accepted = filter.isValid(set);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldAcceptEmptyLists() {
        // Given
        final AreIn filter = new AreIn(VALUE1);

        // When
        boolean accepted = filter.isValid(new ArrayList<>());

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldAcceptEmptySets() {
        // Given
        final AreIn filter = new AreIn(VALUE1);

        // When
        boolean accepted = filter.isValid(new HashSet<>());

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldClone() {
        // Given
        final AreIn filter = new AreIn(VALUE1);

        // When
        final AreIn clonedFilter = filter.statelessClone();

        // Then
        assertNotSame(filter, clonedFilter);
        assertArrayEquals(Collections.singleton(VALUE1).toArray(), clonedFilter.getValues().toArray());
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final AreIn filter = new AreIn(VALUE1);

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.filter.AreIn\",%n" +
                "  \"values\" : [\"value1\"]%n" +
                "}"), json);

        // When 2
        final AreIn deserialisedFilter = new JSONSerialiser().deserialise(json
                .getBytes(), AreIn.class);

        // Then 2
        assertNotNull(deserialisedFilter);
        assertArrayEquals(Collections.singleton(VALUE1).toArray(), deserialisedFilter.getValues().toArray());
    }

    @Override
    protected Class<AreIn> getFunctionClass() {
        return AreIn.class;
    }

    @Override
    protected AreIn getInstance() {
        return new AreIn(VALUE1);
    }
}
