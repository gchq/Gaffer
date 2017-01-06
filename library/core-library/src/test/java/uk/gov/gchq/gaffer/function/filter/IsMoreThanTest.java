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
import uk.gov.gchq.gaffer.function.FilterFunctionTest;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class IsMoreThanTest extends FilterFunctionTest {
    @Test
    public void shouldAcceptTheValueWhenMoreThan() {
        // Given
        final IsMoreThan filter = new IsMoreThan(5);

        // When
        boolean accepted = filter.isValid(6);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldAcceptTheValueWhenMoreThanAndOrEqualToIsTrue() {
        // Given
        final IsMoreThan filter = new IsMoreThan(5, true);

        // When
        boolean accepted = filter.isValid(6);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldRejectTheValueWhenLessThanAndOrEqualToIsTrue() {
        // Given
        final IsMoreThan filter = new IsMoreThan(5, true);

        // When
        boolean accepted = filter.isValid(4);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldRejectTheValueWhenLessThan() {
        // Given
        final IsMoreThan filter = new IsMoreThan(5);

        // When
        boolean accepted = filter.isValid(4);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldRejectTheValueWhenEqual() {
        // Given
        final IsMoreThan filter = new IsMoreThan(5);

        // When
        boolean accepted = filter.isValid(5);

        // Then
        assertFalse(accepted);
    }


    @Test
    public void shouldAcceptTheValueWhenEqualAndOrEqualToIsTrue() {
        // Given
        final IsMoreThan filter = new IsMoreThan(5, true);

        // When
        boolean accepted = filter.isValid(5);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldClone() {
        // Given
        final IsMoreThan filter = new IsMoreThan(5);

        // When
        final IsMoreThan clonedFilter = filter.statelessClone();

        // Then
        assertNotSame(filter, clonedFilter);
        assertEquals(5, clonedFilter.getControlValue());
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final Integer controlValue = 5;
        final IsMoreThan filter = new IsMoreThan(controlValue);

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.filter.IsMoreThan\",%n" +
                "  \"orEqualTo\" : false,%n" +
                "  \"value\" : 5%n" +
                "}"), json);

        // When 2
        final IsMoreThan deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), IsMoreThan.class);

        // Then 2
        assertNotNull(deserialisedFilter);
        assertEquals(controlValue, deserialisedFilter.getControlValue());
    }

    @Override
    protected Class<IsMoreThan> getFunctionClass() {
        return IsMoreThan.class;
    }

    @Override
    protected IsMoreThan getInstance() {
        return new IsMoreThan(5);
    }
}
