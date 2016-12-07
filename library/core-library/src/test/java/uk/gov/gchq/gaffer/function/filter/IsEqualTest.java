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

public class IsEqualTest extends FilterFunctionTest {

    @Test
    public void shouldAcceptTheTestValue() {
        final IsEqual filter = new IsEqual("test");

        boolean accepted = filter.isValid("test");

        assertTrue(accepted);
    }

    @Test
    public void shouldAcceptWhenControlValueAndTestValueAreNull() {
        final IsEqual filter = new IsEqual();

        boolean accepted = filter.isValid((Object) null);

        assertTrue(accepted);
    }

    @Test
    public void shouldRejectWhenNotEqual() {
        final IsEqual filter = new IsEqual("test");

        boolean accepted = filter.isValid("a different value");

        assertFalse(accepted);
    }

    @Test
    public void shouldClone() {
        // Given
        final IsEqual filter = new IsEqual("test");

        // When
        final IsEqual clonedFilter = filter.statelessClone();

        // Then
        assertNotNull(clonedFilter);
        assertNotSame(filter, clonedFilter);
        assertEquals("test", clonedFilter.getControlValue());
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final IsEqual filter = new IsEqual("test");

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.filter.IsEqual\",%n" +
                "  \"value\" : \"test\"%n" +
                "}"), json);

        // When 2
        final IsEqual deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), IsEqual.class);

        // Then 2
        assertNotNull(deserialisedFilter);
    }

    @Override
    protected Class<IsEqual> getFunctionClass() {
        return IsEqual.class;
    }

    @Override
    protected IsEqual getInstance() {
        return new IsEqual("someString");
    }
}
