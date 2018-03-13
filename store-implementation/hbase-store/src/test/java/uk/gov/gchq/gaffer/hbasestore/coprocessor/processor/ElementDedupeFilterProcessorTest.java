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
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.serialisation.LazyElementCell;
import uk.gov.gchq.gaffer.hbasestore.util.CellUtil;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ElementDedupeFilterProcessorTest {
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

    private static final List<Element> ELEMENTS = Arrays.asList(
            new Edge.Builder().group(TestGroups.EDGE)
                    .source("vertexA")
                    .dest("vertexB")
                    .directed(true)
                    .build(),
            new Edge.Builder().group(TestGroups.EDGE)
                    .source("vertexD")
                    .dest("vertexC")
                    .directed(true)
                    .build(),
            new Edge.Builder().group(TestGroups.EDGE)
                    .source("vertexE")
                    .dest("vertexE")
                    .directed(true)
                    .build(),
            new Edge.Builder().group(TestGroups.EDGE)
                    .source("vertexF")
                    .dest("vertexG")
                    .directed(false)
                    .build(),
            new Edge.Builder().group(TestGroups.EDGE)
                    .source("vertexH")
                    .dest("vertexH")
                    .directed(false)
                    .build(),
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("vertexI")
                    .build()
    );

    private final ElementSerialisation serialisation = new ElementSerialisation(SCHEMA);

    @Test
    public void shouldOnlyAcceptEdges() throws OperationException, SerialisationException {
        // Given
        final ElementDedupeFilterProcessor processor = new ElementDedupeFilterProcessor(false, true, DirectedType.EITHER);

        // When / Then
        for (final Element element : ELEMENTS) {
            final boolean expectedResult = element instanceof Edge;
            final Pair<LazyElementCell, LazyElementCell> cells = CellUtil.getLazyCells(element, serialisation);
            assertEquals("Failed for element: " + element.toString(), expectedResult, processor.test(cells.getFirst()));
            if (null != cells.getSecond()) {
                // self elements are not added the other way round
                assertEquals("Failed for element: " + element.toString(), false, processor.test(cells.getSecond()));
            }
        }
    }


    @Test
    public void shouldOnlyAcceptDirectedEdges() throws OperationException, SerialisationException {
        // Given
        final ElementDedupeFilterProcessor processor = new ElementDedupeFilterProcessor(false, true, DirectedType.DIRECTED);

        // When / Then
        for (final Element element : ELEMENTS) {
            final boolean expectedResult = element instanceof Edge && ((Edge) element).isDirected();
            final Pair<LazyElementCell, LazyElementCell> cells = CellUtil.getLazyCells(element, serialisation);
            assertEquals("Failed for element: " + element.toString(), expectedResult, processor.test(cells.getFirst()));
            if (null != cells.getSecond()) {
                // self elements are not added the other way round
                assertEquals("Failed for element: " + element.toString(), false, processor.test(cells.getSecond()));
            }
        }
    }

    @Test
    public void shouldOnlyAcceptUndirectedEdges() throws OperationException, SerialisationException {
        // Given
        final ElementDedupeFilterProcessor processor = new ElementDedupeFilterProcessor(false, true, DirectedType.UNDIRECTED);

        // When / Then
        for (final Element element : ELEMENTS) {
            final boolean expectedResult = element instanceof Edge && !((Edge) element).isDirected();
            final Pair<LazyElementCell, LazyElementCell> cells = CellUtil.getLazyCells(element, serialisation);
            assertEquals("Failed for element: " + element.toString(), expectedResult, processor.test(cells.getFirst()));
            if (null != cells.getSecond()) {
                // self elements are not added the other way round
                assertEquals("Failed for element: " + element.toString(), false, processor.test(cells.getSecond()));
            }
        }
    }

    @Test
    public void shouldAcceptOnlyEntities() throws OperationException, SerialisationException {
        // Given
        final ElementDedupeFilterProcessor processor = new ElementDedupeFilterProcessor(true, false, null);

        // When / Then
        for (final Element element : ELEMENTS) {
            final boolean expectedResult = element instanceof Entity;
            final Pair<LazyElementCell, LazyElementCell> cells = CellUtil.getLazyCells(element, serialisation);
            assertEquals("Failed for element: " + element.toString(), expectedResult, processor.test(cells.getFirst()));
            if (null != cells.getSecond()) {
                // entities and self edges are not added the other way round
                assertEquals("Failed for element: " + element.toString(), expectedResult, processor.test(cells.getSecond()));
            }
        }
    }
}
