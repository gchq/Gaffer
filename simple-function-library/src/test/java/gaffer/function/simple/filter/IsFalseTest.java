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

import gaffer.exception.SerialisationException;
import gaffer.function.FilterFunctionTest;
import gaffer.jsonserialisation.JSONSerialiser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class IsFalseTest extends FilterFunctionTest {
    @Test
    public void shouldAcceptTheValueWhenFalse() {
        // Given
        final IsFalse filter = new IsFalse();

        // When
        boolean accepted = filter._isValid(false);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldAcceptTheValueWhenObjectFalse() {
        // Given
        final IsFalse filter = new IsFalse();

        // When
        boolean accepted = filter._isValid(Boolean.FALSE);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldRejectTheValueWhenNull() {
        // Given
        final IsFalse filter = new IsFalse();

        // When
        boolean accepted = filter._isValid(null);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldRejectTheValueWhenNullItemInArray() {
        // Given
        final IsFalse filter = new IsFalse();

        // When
        boolean accepted = filter._isValid(null);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldRejectTheValueWhenTrue() {
        // Given
        final IsFalse filter = new IsFalse();

        // When
        boolean accepted = filter._isValid(true);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldClone() {
        // Given
        final IsFalse filter = new IsFalse();

        // When
        final IsFalse clonedFilter = filter.statelessClone();

        // Then
        assertNotSame(filter, clonedFilter);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final IsFalse filter = new IsFalse();

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        assertEquals("{\n" +
                "  \"class\" : \"gaffer.function.simple.filter.IsFalse\"\n" +
                "}", json);

        // When 2
        final IsFalse deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), IsFalse.class);

        // Then 2
        assertNotNull(deserialisedFilter);
    }

    @Override
    protected Class<IsFalse> getFunctionClass() {
        return IsFalse.class;
    }

    @Override
    protected IsFalse getInstance() {
        return new IsFalse();
    }

    @Override
    protected Object[] getSomeAcceptedInput() {
        return new Object[]{false};
    }
}
