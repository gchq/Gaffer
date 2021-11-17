/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.types.function;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;
import uk.gov.gchq.koryphe.function.FunctionTest;
import uk.gov.gchq.koryphe.tuple.n.Tuple3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StringsToTypeSubTypeValueTest extends FunctionTest {
    @Test
    public void shouldConvertStringToTypeSubTypeValue() {
        // Given
        final StringsToTypeSubTypeValue function = new StringsToTypeSubTypeValue();

        final String type = "type1";
        final String subtype = "subType1";
        final String value = "value1";

        // When
        final TypeSubTypeValue result = function.apply(new Tuple3<>(type, subtype, value));

        // Then
        assertEquals(new TypeSubTypeValue(type, subtype, value), result);
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final StringsToTypeSubTypeValue function = new StringsToTypeSubTypeValue();

        // When 1
        final String json = new String(JSONSerialiser.serialise(function, true));

        // Then 1
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.types.function.StringsToTypeSubTypeValue\"%n" +
                "}"), json);

        // When 2
        final StringsToTypeSubTypeValue deserialisedFunction = JSONSerialiser.deserialise(json.getBytes(), getFunctionClass());

        // Then 2
        assertNotNull(deserialisedFunction);
    }

    @Override
    protected StringsToTypeSubTypeValue getInstance() {
        return new StringsToTypeSubTypeValue();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }

    @Override
    protected Class<StringsToTypeSubTypeValue> getFunctionClass() {
        return StringsToTypeSubTypeValue.class;
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{String.class, String.class, String.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{TypeSubTypeValue.class};
    }
}
