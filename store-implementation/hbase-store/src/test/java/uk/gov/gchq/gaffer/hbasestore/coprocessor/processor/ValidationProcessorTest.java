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
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
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

public class ValidationProcessorTest {
    private static final Schema SCHEMA = new Schema.Builder()
            .type("string", String.class)
            .type("type", Boolean.class)
            .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                    .source("string")
                    .destination("string")
                    .directed("true")
                    .validateFunctions(new ElementFilter.Builder()
                            .select(IdentifierType.SOURCE.name())
                            .execute("validVertex"::equals)
                            .build())
                    .build())
            .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                    .vertex("string")
                    .validateFunctions(new ElementFilter.Builder()
                            .select(IdentifierType.VERTEX.name())
                            .execute("validVertex"::equals)
                            .build())
                    .build())
            .vertexSerialiser(new StringSerialiser())
            .build();

    private final ElementSerialisation serialisation = new ElementSerialisation(SCHEMA);

    @Test
    public void shouldConstructWithSchema() throws OperationException, SerialisationException {
        final ValidationProcessor processor = new ValidationProcessor(SCHEMA);
        assertEquals(SCHEMA, processor.getSchema());
    }

    @Test
    public void shouldFilterOutInvalidEntity() throws OperationException, SerialisationException {
        // Given
        final ValidationProcessor processor = new ValidationProcessor(SCHEMA);

        // When / Then
        assertFalse(processor.test(CellUtil.getLazyCell(new Entity(TestGroups.ENTITY, "invalidVertex"), serialisation)));
    }

    @Test
    public void shouldNotFilterOutValidEntity() throws OperationException, SerialisationException {
        // Given
        final ValidationProcessor processor = new ValidationProcessor(SCHEMA);

        // When / Then
        assertTrue(processor.test(CellUtil.getLazyCell(new Entity(TestGroups.ENTITY, "validVertex"), serialisation)));
    }

    @Test
    public void shouldFilterOutInvalidEdge() throws OperationException, SerialisationException {
        // Given
        final ValidationProcessor processor = new ValidationProcessor(SCHEMA);

        // When / Then
        assertFalse(processor.test(CellUtil.getLazyCell(
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("invalidVertex")
                        .dest("dest")
                        .directed(true)
                        .build(),
                serialisation)));
    }

    @Test
    public void shouldNotFilterOutValidEdge() throws OperationException, SerialisationException {
        // Given
        final ValidationProcessor processor = new ValidationProcessor(SCHEMA);

        // When / Then
        assertTrue(processor.test(CellUtil.getLazyCell(
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("validVertex")
                        .dest("dest")
                        .directed(true)
                        .build(),
                serialisation)));
    }
}
