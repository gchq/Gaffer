/*
 * Copyright 2018-2021 Crown Copyright
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

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.util.Collections;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ExtractPropertyTest extends FunctionTest {

    @Test
    public void shouldReturnNullForNullElement() {
        final ExtractProperty extractor = new ExtractProperty();

        final Object result = extractor.apply(null);

        assertNull(result);
    }

    @Test
    public void shouldReturnNullWithNoNameProvided() {
        final Element element = mock(Element.class);
        final ExtractProperty extractor = new ExtractProperty();

        final Object result = extractor.apply(element);

        assertNull(result);
    }

    @Test
    public void shouldReturnNullWhenNameNotFoundInElementProperties() {
        final Element element = mock(Element.class);
        final String propName = "absentProperty";

        final ExtractProperty extractor = new ExtractProperty(propName);

        final Object result = extractor.apply(element);

        assertNull(result);
    }

    @Test
    public void shouldReturnValueOfProperty() {
        final Element element = mock(Element.class);
        final String propName = "presentProperty";
        final int propValue = 3;
        final ExtractProperty extractor = new ExtractProperty(propName);

        given(element.getProperty(propName)).willReturn(propValue);

        // When
        final Object result = extractor.apply(element);

        // Then
        assertEquals(propValue, result);
    }

    @Override
    protected ExtractProperty getInstance() {
        return new ExtractProperty("count");
    }

    @Override
    protected Iterable<ExtractProperty> getDifferentInstancesOrNull() {
        return Collections.singletonList(new ExtractProperty("property"));
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return ExtractProperty.class;
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Element.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{Object.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        final ExtractProperty function = getInstance();

        final byte[] json = JSONSerialiser.serialise(function);
        final ExtractProperty deserialisedObj = JSONSerialiser.deserialise(json, ExtractProperty.class);

        final String expectedJson = "{\"class\":\"uk.gov.gchq.gaffer.data.element.function.ExtractProperty\",\"name\":\"count\"}";
        JsonAssert.assertEquals(expectedJson, new String(json));
        assertEquals("count", deserialisedObj.getName());
    }
}
