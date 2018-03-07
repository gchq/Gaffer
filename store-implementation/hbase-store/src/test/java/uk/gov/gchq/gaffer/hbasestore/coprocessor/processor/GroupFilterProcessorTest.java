/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.hbasestore.coprocessor.processor;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.util.CellUtil;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GroupFilterProcessorTest {
    private static final Schema SCHEMA = new Schema.Builder()
            .type("string", String.class)
            .type("type", Boolean.class)
            .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                    .source("string")
                    .destination("string")
                    .directed("true")
                    .build())
            .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition.Builder()
                    .source("string")
                    .destination("string")
                    .directed("true")
                    .build())
            .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                    .vertex("string")
                    .build())
            .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                    .vertex("string")
                    .build())
            .vertexSerialiser(new StringSerialiser())
            .build();

    private static final View VIEW = new View.Builder()
            .entity(TestGroups.ENTITY_2)
            .edge(TestGroups.EDGE_2)
            .build();

    private final ElementSerialisation serialisation = new ElementSerialisation(SCHEMA);

    @Test
    public void shouldConstructWithView() throws OperationException, SerialisationException {
        final GroupFilterProcessor processor = new GroupFilterProcessor(VIEW);
        assertEquals(VIEW, processor.getView());
    }

    @Test
    public void shouldFilterOutEntityNotInView() throws OperationException, SerialisationException {
        // Given
        final GroupFilterProcessor processor = new GroupFilterProcessor(VIEW);

        // When
        final boolean result = processor.test(CellUtil.getLazyCell(new Entity(TestGroups.ENTITY, "vertexI"), serialisation));

        // Then
        assertFalse(result);
    }

    @Test
    public void shouldNotFilterOutEntityInView() throws OperationException, SerialisationException {
        // Given
        final GroupFilterProcessor processor = new GroupFilterProcessor(VIEW);

        // When
        final boolean result = processor.test(CellUtil.getLazyCell(new Entity(TestGroups.ENTITY_2, "vertexI"), serialisation));

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldFilterOutEdgeNotInView() throws OperationException, SerialisationException {
        // Given
        final GroupFilterProcessor processor = new GroupFilterProcessor(VIEW);

        // When
        final boolean result = processor.test(CellUtil.getLazyCell(
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("A")
                        .dest("B")
                        .directed(true)
                        .build(),
                serialisation));

        // Then
        assertFalse(result);
    }

    @Test
    public void shouldNotFilterOutEdgeInView() throws OperationException, SerialisationException {
        // Given
        final GroupFilterProcessor processor = new GroupFilterProcessor(VIEW);

        // When
        final boolean result = processor.test(CellUtil.getLazyCell(
                new Edge.Builder()
                        .group(TestGroups.EDGE_2)
                        .source("A")
                        .dest("B")
                        .directed(true)
                        .build(),
                serialisation));
        // Then
        assertTrue(result);
    }
}
