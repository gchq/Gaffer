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
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class UnwrapEntityIdTest extends GafferFunctionTest {

    @Test
    public void shouldReturnNullForNullValue() {
        final UnwrapEntityId function = new UnwrapEntityId();

        final Object result = function.apply(null);

        assertNull(result);
    }

    @Test
    public void shouldReturnOriginalValueForEdgeIds() {
        final EdgeId value = mock(EdgeId.class);
        final UnwrapEntityId function = new UnwrapEntityId();

        final Object result = function.apply(value);

        assertSame(value, result);
    }

    @Test
    public void shouldUnwrapEntityIds() {
        final EntityId value = mock(EntityId.class);
        final Object vertex = mock(Object.class);
        given(value.getVertex()).willReturn(vertex);

        final UnwrapEntityId function = new UnwrapEntityId();

        final Object result = function.apply(value);

        assertSame(vertex, result);
    }

    @Override
    protected UnwrapEntityId getInstance() {
        return new UnwrapEntityId();
    }

    @Override
    protected Class<? extends UnwrapEntityId> getFunctionClass() {
        return UnwrapEntityId.class;
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        final UnwrapEntityId function = getInstance();

        final byte[] json = JSONSerialiser.serialise(function);
        final UnwrapEntityId deserialisedObj = JSONSerialiser.deserialise(json, UnwrapEntityId.class);

        final String expectedJson = "{\"class\":\"uk.gov.gchq.gaffer.data.element.function.UnwrapEntityId\"}";
        JsonAssert.assertEquals(expectedJson, new String(json));
        assertNotNull(deserialisedObj);
    }
}
