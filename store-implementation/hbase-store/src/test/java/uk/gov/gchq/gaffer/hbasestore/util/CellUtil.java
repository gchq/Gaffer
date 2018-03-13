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

package uk.gov.gchq.gaffer.hbasestore.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.serialisation.LazyElementCell;

import java.util.ArrayList;
import java.util.List;

public final class CellUtil {
    private CellUtil() {
    }

    public static List<Element> getElements(final Iterable<Put> puts, final ElementSerialisation serialisation) throws SerialisationException {
        return getElements(puts, serialisation, false);
    }

    public static List<Element> getElements(final Iterable<Put> puts, final ElementSerialisation serialisation, final boolean includeMatchedVertex) throws SerialisationException {
        final List<Element> cells = new ArrayList<>();
        for (final Put put : puts) {
            cells.add(getLazyCell(put, serialisation, includeMatchedVertex).getElement());
        }

        return cells;
    }

    public static List<LazyElementCell> getLazyCellsFromPuts(final Iterable<Put> puts, final ElementSerialisation serialisation) throws SerialisationException {
        return getLazyCellsFromPuts(puts, serialisation, false);
    }

    public static List<LazyElementCell> getLazyCellsFromPuts(final Iterable<Put> puts, final ElementSerialisation serialisation, final boolean includeMatchedVertex) throws SerialisationException {
        final List<LazyElementCell> cells = new ArrayList<>();
        for (final Put put : puts) {
            cells.add(getLazyCell(put, serialisation, includeMatchedVertex));
        }

        return cells;
    }

    public static List<LazyElementCell> getLazyCells(final Iterable<Element> elements, final ElementSerialisation serialisation) throws SerialisationException {
        return getLazyCells(elements, serialisation, false);
    }

    public static List<LazyElementCell> getLazyCells(final Iterable<Element> elements, final ElementSerialisation serialisation, final boolean includeMatchedVertex) throws SerialisationException {
        final List<LazyElementCell> cells = new ArrayList<>();
        for (final Element element : elements) {
            final Pair<LazyElementCell, LazyElementCell> cellPair = getLazyCells(element, serialisation, includeMatchedVertex);
            cells.add(cellPair.getFirst());
            if (null != cellPair.getSecond()) {
                cells.add(cellPair.getSecond());
            }
        }

        return cells;
    }

    public static LazyElementCell getLazyCell(final Element element, final ElementSerialisation serialisation) throws SerialisationException {
        return getLazyCell(element, serialisation, false);
    }

    public static LazyElementCell getLazyCell(final Element element, final ElementSerialisation serialisation, final boolean includeMatchedVertex) throws SerialisationException {
        return getLazyCells(serialisation.getPuts(element), serialisation, includeMatchedVertex).getFirst();
    }

    public static Pair<LazyElementCell, LazyElementCell> getLazyCells(final Element element, final ElementSerialisation serialisation) throws SerialisationException {
        return getLazyCells(element, serialisation, false);
    }

    public static Pair<LazyElementCell, LazyElementCell> getLazyCells(final Element element, final ElementSerialisation serialisation, final boolean includeMatchedVertex) throws SerialisationException {
        return getLazyCells(serialisation.getPuts(element), serialisation, includeMatchedVertex);
    }

    public static LazyElementCell getLazyCell(final Put put, final ElementSerialisation serialisation, final boolean includeMatchedVertex) {
        return new LazyElementCell(getCell(put), serialisation, includeMatchedVertex);
    }

    public static Pair<LazyElementCell, LazyElementCell> getLazyCells(final Pair<Put, Put> puts, final ElementSerialisation serialisation) {
        return getLazyCells(puts, serialisation, false);
    }

    public static Pair<LazyElementCell, LazyElementCell> getLazyCells(final Pair<Put, Put> puts, final ElementSerialisation serialisation, final boolean includeMatchedVertex) {
        final Pair<Cell, Cell> cells = getCells(puts);
        final Pair<LazyElementCell, LazyElementCell> lazyCells = new Pair<>();
        lazyCells.setFirst(new LazyElementCell(cells.getFirst(), serialisation, includeMatchedVertex));
        if (null != cells.getSecond()) {
            lazyCells.setSecond(new LazyElementCell(cells.getSecond(), serialisation, includeMatchedVertex));
        }
        return lazyCells;
    }

    public static List<Cell> getCells(final Iterable<Element> elements, final ElementSerialisation serialisation) throws SerialisationException {
        final List<Cell> cells = new ArrayList<>();
        for (final Element element : elements) {
            final Pair<Cell, Cell> cellPair = getCells(element, serialisation);
            cells.add(cellPair.getFirst());
            if (null != cellPair.getSecond()) {
                cells.add(cellPair.getSecond());
            }
        }

        return cells;
    }

    public static Cell getCell(final Element element, final ElementSerialisation serialisation) throws SerialisationException {
        return getCells(serialisation.getPuts(element)).getFirst();
    }

    public static Pair<Cell, Cell> getCells(final Element element, final ElementSerialisation serialisation) throws SerialisationException {
        return getCells(serialisation.getPuts(element));
    }

    public static Cell getCell(final Put put) {
        return put.getFamilyCellMap().values().iterator().next().iterator().next();
    }

    public static Pair<Cell, Cell> getCells(final Pair<Put, Put> puts) {
        final Pair<Cell, Cell> cells = new Pair<>();
        cells.setFirst(getCell(puts.getFirst()));

        if (null != puts.getSecond()) {
            cells.setSecond(getCell(puts.getSecond()));
        }

        return cells;
    }
}
