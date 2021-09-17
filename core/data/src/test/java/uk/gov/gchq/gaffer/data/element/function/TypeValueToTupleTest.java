/*
 * Copyright 2021 Crown Copyright
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
package uk.gov.gchq.gaffer.data.element.function;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.TypeValue;
import uk.gov.gchq.koryphe.function.FunctionComposite;
import uk.gov.gchq.koryphe.function.FunctionTest;
import uk.gov.gchq.koryphe.impl.function.Length;
import uk.gov.gchq.koryphe.impl.function.ToString;
import uk.gov.gchq.koryphe.tuple.Tuple;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunctionComposite;

import java.util.Arrays;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TypeValueToTupleTest extends FunctionTest<TypeValueToTuple> {

    @Test
    public void shouldHandleNullInput() {
        // Given
        final TypeValueToTuple function = getInstance();

        // When
        final Tuple<String> result = function.apply(null);

        // Then
        assertNull(result);
    }

    @Test
    public void shouldConvertTypeValueToTuple() {
        // Given
        final TypeValue typeValue = new TypeValue("type", "value");
        final TypeValueToTuple function = getInstance();

        // When
        final Tuple<String> result = function.apply(typeValue);

        // Then
        assertEquals("type", result.get("type"));
        assertEquals("value", result.get("value"));
    }

    @Test
    public void shouldGetAndSetUsingCompositeFunction() {
        // Given
        final TypeValue typeValue = new TypeValue("type", "value");
        final Function<Object, Object> compositeFunction = new FunctionComposite(Lists.newArrayList(
                new TypeValueToTuple(),
                new TupleAdaptedFunctionComposite.Builder()
                        .select(new String[]{"value"})
                        .execute(new FunctionComposite(Arrays.asList(
                                new Length(),
                                new ToString()
                        )))
                        .project(new String[]{"type"})
                        .build()
        ));

        // When
        compositeFunction.apply(typeValue);

        // Then
        assertEquals(new TypeValue("5", "value"), typeValue);
    }

    @Override
    protected TypeValueToTuple getInstance() {
        return new TypeValueToTuple();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }

    @Override
    protected Class<? extends TypeValueToTuple> getFunctionClass() {
        return TypeValueToTuple.class;
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{TypeValue.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{TypeValueTuple.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final TypeValueToTuple function = getInstance();

        // When
        final byte[] json = JSONSerialiser.serialise(function);
        final TypeValueToTuple deserialisedObj = JSONSerialiser.deserialise(json, TypeValueToTuple.class);

        // Then
        JsonAssert.assertEquals(
                "{\"class\":\"uk.gov.gchq.gaffer.data.element.function.TypeValueToTuple\"}",
                new String(json)
        );
        assertNotNull(deserialisedObj);
    }
}
