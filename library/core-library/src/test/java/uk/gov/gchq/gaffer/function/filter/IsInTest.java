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

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.predicate.PredicateTest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class IsInTest extends PredicateTest {
    @Test
    public void shouldAcceptWhenValueInList() {
        // Given
        final IsIn filter = new IsIn(Arrays.asList((Object) "A", "B", "C"));

        // When
        boolean accepted = filter.test("B");

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldRejectWhenValueNotInList() {
        // Given
        final IsIn filter = new IsIn(Arrays.asList((Object) "A", "B", "C"));

        // When
        boolean accepted = filter.test("D");

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final Object[] controlData = {1, 2, 3};
        final IsIn filter = new IsIn(Arrays.asList(controlData));

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.filter.IsIn\",%n" +
                "  \"values\" : [ 1, 2, 3 ]%n" +
                "}"), json);

        // When 2
        final IsIn deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), IsIn.class);

        // Then 2
        assertNotNull(deserialisedFilter);
        assertArrayEquals(controlData, deserialisedFilter.getAllowedValuesArray());
    }

    @Override
    protected Class<IsIn> getPredicateClass() {
        return IsIn.class;
    }

    @Override
    protected IsIn getInstance() {
        return new IsIn(Collections.singletonList((Object) "someValue"));
    }
}
