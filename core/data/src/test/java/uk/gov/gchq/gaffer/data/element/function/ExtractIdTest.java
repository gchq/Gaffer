/*
 * Copyright 2017-2020 Crown Copyright
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
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ExtractIdTest extends FunctionTest {

    @Test
    public void shouldReturnNullForNullElement() {
        final ExtractId extractor = new ExtractId();

        final Object result = extractor.apply(null);

        assertNull(result);
    }

    @Test
    public void shouldReturnNullWithNoIdentifierTypeProvided() {
        final Element element = mock(Element.class);
        final ExtractId extractor = new ExtractId();

        final Object result = extractor.apply(element);

        assertNull(result);
    }

    @Test
    public void shouldReturnNullWhenIdentifierTypeNotFoundInElement() {
        final Element element = mock(Element.class);
        final IdentifierType type = IdentifierType.VERTEX;
        final ExtractId extractor = new ExtractId(type);

        final Object result = extractor.apply(element);

        assertNull(result);
    }

    @Test
    public void shouldReturnValueOfIdentifierType() {
        final Element element = mock(Element.class);
        final IdentifierType type = IdentifierType.SOURCE;
        final String value = "testSource";

        final ExtractId extractor = new ExtractId(type);

        given(element.getIdentifier(type)).willReturn(value);

        // When
        final Object result = extractor.apply(element);

        assertEquals(value, result);
    }

    @Override
    protected ExtractId getInstance() {
        return new ExtractId(IdentifierType.SOURCE);
    }

    @Override
    protected Iterable<ExtractId> getDifferentInstancesOrNull() {
        return Collections.singletonList(new ExtractId(IdentifierType.DESTINATION));
    }

    @Override
    protected Class<? extends ExtractId> getFunctionClass() {
        return ExtractId.class;
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
        final ExtractId function = getInstance();

        final byte[] json = JSONSerialiser.serialise(function);
        final ExtractId deserialisedObj = JSONSerialiser.deserialise(json, ExtractId.class);

        final String expectedJson = "{\"class\":\"uk.gov.gchq.gaffer.data.element.function.ExtractId\",\"id\":\"SOURCE\"}";
        JsonAssert.assertEquals(expectedJson, new String(json));
        assertEquals(IdentifierType.SOURCE, deserialisedObj.getId());
    }
}
