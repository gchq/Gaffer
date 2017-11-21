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

package uk.gov.gchq.gaffer.operation.impl;

import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;


public class ValidateTest extends OperationTest<Validate> {
    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final List<Element> elements = Arrays.asList(
                new Entity.Builder()
                        .group("entity type 1")
                        .vertex("vertex 1")
                        .property("property 1", "property 1 value")
                        .build(),
                new Edge.Builder().group("edge type 2")
                        .source("source vertex 1")
                        .dest("dest vertex 1")
                        .directed(true)
                        .property("property 2", "property 2 value")
                        .build()
        );

        final Validate op = new Validate.Builder()
                .validate(true)
                .skipInvalidElements(true)
                .build();
        op.setInput(elements);

        // When
        byte[] json = JSONSerialiser.serialise(op, true);
        final Validate deserialisedOp = JSONSerialiser.deserialise(json, Validate.class);

        // Then
        final Iterator<? extends Element> itr = deserialisedOp.getInput().iterator();

        final Entity elm1 = (Entity) itr.next();
        assertEquals("vertex 1", elm1.getVertex());
        assertEquals(1, elm1.getProperties().size());
        assertEquals("property 1 value", elm1.getProperty("property 1"));

        final Edge elm2 = (Edge) itr.next();
        assertEquals("source vertex 1", elm2.getSource());
        assertEquals("dest vertex 1", elm2.getDestination());
        assertTrue(elm2.isDirected());
        assertEquals(1, elm2.getProperties().size());
        assertEquals("property 2 value", elm2.getProperty("property 2"));

        assertFalse(itr.hasNext());

        assertTrue(deserialisedOp.isSkipInvalidElements());
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final Element edge = new Edge.Builder()
                .group("testGroup")
                .build();
        final Validate validate = new Validate.Builder()
                .input(edge)
                .skipInvalidElements(true)
                .build();
        assertTrue(validate.isSkipInvalidElements());
        assertEquals(edge, validate.getInput().iterator().next());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final Element input = new Edge.Builder()
                .group("testGroup")
                .build();
        final Validate validate = new Validate.Builder()
                .input(input)
                .skipInvalidElements(true)
                .validate(true)
                .build();

        // When
        final Validate clone = validate.shallowClone();

        // Then
        assertNotSame(validate, clone);
        assertTrue(clone.isSkipInvalidElements());
        assertTrue(clone.isValidate());
        assertEquals(input, clone.getInput().iterator().next());
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertEquals(Iterable.class, outputClass);
    }

    @Override
    protected Validate getTestObject() {
        return new Validate();
    }
}