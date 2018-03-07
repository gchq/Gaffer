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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
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
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class FilterProcessorTest {
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
            new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("vertexI")
                    .build(),
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
                    .build()
    );

    private final ElementSerialisation serialisation = new ElementSerialisation(SCHEMA);

    @Test
    public void shouldSkipDeletedAndRemoveInvalidElements() throws OperationException, SerialisationException {
        // Given
        final FilterProcessor processor = new FilterProcessor() {
            @Override
            public boolean test(final LazyElementCell elementCell) {
                return elementCell.getElement() instanceof Entity;
            }
        };

        final List<LazyElementCell> lazyCells = CellUtil.getLazyCells(ELEMENTS, serialisation);
        final Cell deletedCell = mock(Cell.class);
        given(deletedCell.getTypeByte()).willReturn(KeyValue.Type.Delete.getCode());
        lazyCells.add(new LazyElementCell(deletedCell, serialisation, false));

        // When
        final List<LazyElementCell> result = processor.process(lazyCells);

        // When / Then
        assertEquals(2, result.size());
        assertEquals(ELEMENTS.get(0), result.get(0).getElement());
        assertEquals(deletedCell, result.get(1).getCell());
    }
}
