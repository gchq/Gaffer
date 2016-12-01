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
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class ValidateTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final List<Element> elements = new ArrayList<>();
        {
            final Entity elm1 = new Entity("entity type 1", "vertex 1");
            elm1.putProperty("property 1", "property 1 value");
            elements.add(elm1);

        }
        {
            final Edge elm2 = new Edge("edge type 2", "source vertex 1", "dest vertex 1", true);
            elm2.putProperty("property 2", "property 2 value");
            elements.add(elm2);
        }

        final Validate op = new Validate(true);
        op.setElements(elements);

        // When
        byte[] json = serialiser.serialise(op, true);
        final Validate deserialisedOp = serialiser.deserialise(json, Validate.class);

        // Then
        final Iterator<Element> itr = deserialisedOp.getElements().iterator();

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
        Element edge = new Edge("testGroup");
        Validate validate = new Validate.Builder().elements(Arrays.asList(edge)).skipInvalidElements(true).view(new View.Builder().edge("testEdgeGroup").build()).option("testOption", "true").build();
        assertEquals("true", validate.getOption("testOption"));
        assertTrue(validate.isSkipInvalidElements());
        assertEquals(edge, validate.getInput().iterator().next());
        assertNotNull(validate.getView());
    }
}