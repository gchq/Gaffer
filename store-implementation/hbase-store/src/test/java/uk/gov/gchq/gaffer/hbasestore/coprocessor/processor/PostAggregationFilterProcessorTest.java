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
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
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

public class PostAggregationFilterProcessorTest {
    private static final Schema SCHEMA = new Schema.Builder()
            .type("string", String.class)
            .type("type", Boolean.class)
            .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                    .source("string")
                    .destination("string")
                    .directed("true")
                    .build())
            .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                    .vertex("string")
                    .build())
            .vertexSerialiser(new StringSerialiser())
            .build();

    private static final View VIEW = new View.Builder()
            .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                    .preAggregationFilter(new ElementFilter.Builder()
                            .select(IdentifierType.VERTEX.name())
                            .execute("validPreAggVertex"::equals)
                            .build())
                    .postAggregationFilter(new ElementFilter.Builder()
                            .select(IdentifierType.VERTEX.name())
                            .execute("validPostAggVertex"::equals)
                            .build())
                    .postTransformFilter(new ElementFilter.Builder()
                            .select(IdentifierType.VERTEX.name())
                            .execute("validPostTransformVertex"::equals)
                            .build())
                    .build())
            .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                    .preAggregationFilter(new ElementFilter.Builder()
                            .select(IdentifierType.SOURCE.name())
                            .execute("validPreAggVertex"::equals)
                            .build())
                    .postAggregationFilter(new ElementFilter.Builder()
                            .select(IdentifierType.SOURCE.name())
                            .execute("validPostAggVertex"::equals)
                            .build())
                    .postTransformFilter(new ElementFilter.Builder()
                            .select(IdentifierType.SOURCE.name())
                            .execute("validPostTransformVertex"::equals)
                            .build())
                    .build())
            .build();

    private final ElementSerialisation serialisation = new ElementSerialisation(SCHEMA);

    @Test
    public void shouldConstructWithView() throws OperationException, SerialisationException {
        final PostAggregationFilterProcessor processor = new PostAggregationFilterProcessor(VIEW);
        assertEquals(VIEW, processor.getView());
    }

    @Test
    public void shouldFilterOutInvalidEntity() throws OperationException, SerialisationException {
        // Given
        final PostAggregationFilterProcessor processor = new PostAggregationFilterProcessor(VIEW);

        // When / Then
        assertFalse(processor.test(CellUtil.getLazyCell(new Entity(TestGroups.ENTITY, "invalidVertex"), serialisation)));
        assertFalse(processor.test(CellUtil.getLazyCell(new Entity(TestGroups.ENTITY, "validPreAggVertex"), serialisation)));
        assertFalse(processor.test(CellUtil.getLazyCell(new Entity(TestGroups.ENTITY, "validPostTransformVertex"), serialisation)));
    }

    @Test
    public void shouldNotFilterOutValidEntity() throws OperationException, SerialisationException {
        // Given
        final PostAggregationFilterProcessor processor = new PostAggregationFilterProcessor(VIEW);

        // When / Then
        assertTrue(processor.test(CellUtil.getLazyCell(new Entity(TestGroups.ENTITY, "validPostAggVertex"), serialisation)));
    }

    @Test
    public void shouldFilterOutInvalidEdge() throws OperationException, SerialisationException {
        // Given
        final PostAggregationFilterProcessor processor = new PostAggregationFilterProcessor(VIEW);

        // When / Then
        assertFalse(processor.test(CellUtil.getLazyCell(
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("invalidVertex")
                        .dest("dest")
                        .directed(true)
                        .build(),
                serialisation)));
        assertFalse(processor.test(CellUtil.getLazyCell(
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("validPreAggVertex")
                        .dest("dest")
                        .directed(true)
                        .build(),
                serialisation)));
        assertFalse(processor.test(CellUtil.getLazyCell(
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("validPostTransformVertex")
                        .dest("dest")
                        .directed(true)
                        .build(),
                serialisation)));
    }

    @Test
    public void shouldNotFilterOutValidEdge() throws OperationException, SerialisationException {
        // Given
        final PostAggregationFilterProcessor processor = new PostAggregationFilterProcessor(VIEW);

        // When / Then
        assertTrue(processor.test(CellUtil.getLazyCell(
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("validPostAggVertex")
                        .dest("dest")
                        .directed(true)
                        .build(),
                serialisation)));
    }

}
