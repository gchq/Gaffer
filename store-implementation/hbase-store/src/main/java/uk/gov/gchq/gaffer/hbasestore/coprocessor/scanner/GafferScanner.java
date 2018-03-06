/*
 * Copyright 2016-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.hbasestore.coprocessor.processor.GafferScannerProcessor;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.serialisation.LazyElementCell;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class GafferScanner implements InternalScanner {
    private final InternalScanner scanner;
    private final ElementSerialisation serialisation;
    private final List<GafferScannerProcessor> processors;
    private final boolean includeMatchedVertex;

    public GafferScanner(final InternalScanner scanner,
                         final ElementSerialisation serialisation,
                         final List<GafferScannerProcessor> processors,
                         final boolean includeMatchedVertex) {
        this.scanner = scanner;
        this.serialisation = serialisation;
        if (null == processors) {
            this.processors = Collections.emptyList();
        } else {
            this.processors = processors;
        }
        this.includeMatchedVertex = includeMatchedVertex;
    }

    @Override
    public boolean next(final List<Cell> output) throws IOException {
        final List<Cell> input = new ArrayList<>();
        final boolean shouldContinue = scanner.next(input);
        _next(input, output);
        return shouldContinue;
    }

    @Override
    public boolean next(final List<Cell> output, final ScannerContext scannerContext) throws IOException {
        return next(output);
    }

    @Override
    public void close() throws IOException {
        scanner.close();
    }

    protected void _next(final List<Cell> input, final List<Cell> output) throws IOException {
        List<LazyElementCell> elementCells = new ArrayList<>(input.size());
        for (final Cell cell : input) {
            elementCells.add(new LazyElementCell(cell, serialisation, includeMatchedVertex));
        }

        for (final GafferScannerProcessor processor : processors) {
            elementCells = processor.process(elementCells);
        }

        for (final LazyElementCell elementCell : elementCells) {
            output.add(elementCell.getCell());
        }
    }

    protected InternalScanner getScanner() {
        return scanner;
    }
}
