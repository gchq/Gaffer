/*
 * Copyright 2019-2020 Crown Copyright
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
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.ElementTuple;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ToElementTupleTest extends FunctionTest {

    @Test
    public void shouldReturnNullForNullValue() {
        final ToElementTuple function = new ToElementTuple();

        final Object result = function.apply(null);

        assertNull(result);
    }

    @Test
    public void shouldConvertAnElementIntoAnElementTuple() {
        final Element element = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("vertex1")
                .property(TestPropertyNames.COUNT, 1)
                .build();
        final ToElementTuple function = new ToElementTuple();

        final Object result = function.apply(element);

        assertEquals(new ElementTuple(element), result);
    }

    @Override
    protected ToElementTuple getInstance() {
        return new ToElementTuple();
    }

    @Override
    protected Iterable<ToElementTuple> getDifferentInstancesOrNull() {
        return null;
    }

    @Override
    protected Class<? extends ToElementTuple> getFunctionClass() {
        return ToElementTuple.class;
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[] {Element.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[] {ElementTuple.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        final ToElementTuple function = getInstance();

        final byte[] json = JSONSerialiser.serialise(function);
        final ToElementTuple deserialisedObj = JSONSerialiser.deserialise(json, ToElementTuple.class);

        final String expectedJson = "{\"class\":\"uk.gov.gchq.gaffer.data.element.function.ToElementTuple\"}";
        JsonAssert.assertEquals(expectedJson, new String(json));
        assertNotNull(deserialisedObj);
    }
}
