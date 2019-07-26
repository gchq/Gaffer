/*
 * Copyright 2019 Crown Copyright
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

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.ElementTuple;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ToElementTupleTest extends FunctionTest {
    @Test
    public void shouldReturnNullForNullValue() {
        // Given
        final ToElementTuple function = new ToElementTuple();

        // When
        final Object result = function.apply(null);

        // Then
        assertNull(result);
    }

    @Test
    public void shouldConvertAnElementIntoAnElementTuple() {
        // Given
        final Element element = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("vertex1")
                .property(TestPropertyNames.COUNT, 1)
                .build();
        final ToElementTuple function = new ToElementTuple();

        // When
        final Object result = function.apply(element);

        // Then
        assertEquals(new ElementTuple(element), result);
    }

    @Override
    protected ToElementTuple getInstance() {
        return new ToElementTuple();
    }

    @Override
    protected Class<? extends ToElementTuple> getFunctionClass() {
        return ToElementTuple.class;
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final ToElementTuple function = getInstance();

        // When
        final byte[] json = JSONSerialiser.serialise(function);
        final ToElementTuple deserialisedObj = JSONSerialiser.deserialise(json, ToElementTuple.class);

        // Then
        JsonAssert.assertEquals(
                "{\"class\":\"uk.gov.gchq.gaffer.data.element.function.ToElementTuple\"}",
                new String(json)
        );
        assertNotNull(deserialisedObj);
    }
}
