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

package uk.gov.gchq.gaffer.operation.impl.output;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices.EdgeVertices;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices.UseMatchedVertex;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;


public class ToVerticesTest extends OperationTest<ToVertices> {
    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException, JsonProcessingException {
        // Given
        final ToVertices op = new ToVertices.Builder()
                .input(new EntitySeed("2"))
                .edgeVertices(EdgeVertices.BOTH)
                .useMatchedVertex(ToVertices.UseMatchedVertex.OPPOSITE)
                .build();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);
        final ToVertices deserialisedOp = JSONSerialiser.deserialise(json, ToVertices.class);

        // Then
        assertNotNull(deserialisedOp);
    }

    @Test
    public void shouldSerialiseAndDeserialiseOperationWithMissingEdgeVertices() throws SerialisationException, JsonProcessingException {
        // Given
        final ToVertices op = new ToVertices.Builder()
                .input(new EntitySeed("2"))
                .build();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);
        final ToVertices deserialisedOp = JSONSerialiser.deserialise(json, ToVertices.class);

        // Then
        assertNotNull(deserialisedOp);
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final ToVertices toVertices = new ToVertices.Builder()
                .input(new Entity(TestGroups.ENTITY), new Entity(TestGroups.ENTITY_2))
                .edgeVertices(EdgeVertices.BOTH)
                .build();

        // Then
        assertThat(toVertices.getInput(), is(notNullValue()));
        assertThat(toVertices.getInput(), iterableWithSize(2));
        assertThat(toVertices.getEdgeVertices(), is(EdgeVertices.BOTH));
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final Entity input = new Entity(TestGroups.ENTITY);
        final ToVertices toVertices = new ToVertices.Builder()
                .input(input)
                .useMatchedVertex(UseMatchedVertex.EQUAL)
                .edgeVertices(EdgeVertices.BOTH)
                .build();

        // When
        final ToVertices clone = toVertices.shallowClone();

        // Then
        assertNotSame(toVertices, clone);
        assertEquals(input, clone.getInput().iterator().next());
        assertEquals(UseMatchedVertex.EQUAL, clone.getUseMatchedVertex());
        assertEquals(EdgeVertices.BOTH, clone.getEdgeVertices());
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertEquals(Iterable.class, outputClass);
    }

    @Override
    protected ToVertices getTestObject() {
        return new ToVertices();
    }
}
