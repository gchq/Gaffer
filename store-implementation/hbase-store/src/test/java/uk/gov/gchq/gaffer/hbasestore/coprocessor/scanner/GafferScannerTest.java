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

package uk.gov.gchq.gaffer.hbasestore.coprocessor.scanner;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.hbasestore.coprocessor.processor.GafferScannerProcessor;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.serialisation.LazyElementCell;
import uk.gov.gchq.gaffer.hbasestore.util.CellUtil;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class GafferScannerTest {
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
                    .vertex("a")
                    .build(),
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("b")
                    .dest("c")
                    .directed(true)
                    .build()
    );

    private final ElementSerialisation serialisation = new ElementSerialisation(SCHEMA);

    @Test
    public void shouldDelegateToEachProcessor() throws OperationException, IOException {
        // Given
        final List<LazyElementCell> lazyCells = CellUtil.getLazyCells(ELEMENTS, serialisation);
        final List<Cell> cells = new ArrayList<>();
        for (final LazyElementCell lazyElementCell : lazyCells) {
            cells.add(lazyElementCell.getCell());
        }

        final InternalScanner internalScanner = new InternalScanner() {
            @Override
            public boolean next(final List<Cell> results) throws IOException {
                results.addAll(cells);
                return true;
            }

            @Override
            public boolean next(final List<Cell> result, final ScannerContext scannerContext) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() throws IOException {
                throw new UnsupportedOperationException();
            }
        };

        final GafferScannerProcessor processor1 = mock(GafferScannerProcessor.class);
        final GafferScannerProcessor processor2 = mock(GafferScannerProcessor.class);
        final GafferScanner scanner = new GafferScanner(internalScanner, serialisation, Arrays.asList(processor1, processor2), false) {
        };
        final List<LazyElementCell> processedCells1 = mock(List.class);
        given(processor1.process(Mockito.anyList())).willReturn(processedCells1);
        given(processor2.process(processedCells1)).willReturn(lazyCells);


        final List<Cell> outputResult = new ArrayList<>();

        // When
        final boolean result = scanner.next(outputResult, null);

        // When / Then
        assertTrue(result);
        final ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(processor1).process(captor.capture());
        assertEquals(lazyCells, captor.getValue());
        verify(processor2).process(processedCells1);
        assertEquals(cells, outputResult);
    }

    @Test
    public void shouldCloseScanner() throws IOException {
        // Given
        final InternalScanner internalScanner = mock(InternalScanner.class);
        final GafferScanner scanner = new GafferScanner(internalScanner, serialisation, null, false) {
        };

        // When
        scanner.close();

        // Then
        verify(internalScanner).close();
        assertSame(internalScanner, scanner.getScanner());
    }
}
