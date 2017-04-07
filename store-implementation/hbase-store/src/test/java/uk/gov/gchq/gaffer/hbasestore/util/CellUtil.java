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

package uk.gov.gchq.gaffer.hbasestore.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import uk.gov.gchq.gaffer.commonutil.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.serialisation.LazyElementCell;
import java.util.ArrayList;
import java.util.List;

public final class CellUtil {
    private CellUtil() {
    }

    public static List<LazyElementCell> getLazyCells(final Iterable<Element> elements, final ElementSerialisation serialisation) throws SerialisationException {
        final List<LazyElementCell> cells = new ArrayList<>();
        for (final Element element : elements) {
            final Pair<LazyElementCell> cellPair = getLazyCells(element, serialisation);
            cells.add(cellPair.getFirst());
            if (null != cellPair.getSecond()) {
                cells.add(cellPair.getSecond());
            }
        }

        return cells;
    }

    public static LazyElementCell getLazyCell(final Element element, final ElementSerialisation serialisation) throws SerialisationException {
        return getLazyCells(serialisation.getPuts(element), serialisation).getFirst();
    }

    public static Pair<LazyElementCell> getLazyCells(final Element element, final ElementSerialisation serialisation) throws SerialisationException {
        return getLazyCells(serialisation.getPuts(element), serialisation);
    }

    public static Pair<LazyElementCell> getLazyCells(final Pair<Put> puts, final ElementSerialisation serialisation) {
        final Pair<Cell> cells = getCells(puts);
        final Pair<LazyElementCell> lazyCells = new Pair<>();
        lazyCells.setFirst(new LazyElementCell(cells.getFirst(), serialisation));
        if (null != cells.getSecond()) {
            lazyCells.setSecond(new LazyElementCell(cells.getSecond(), serialisation));
        }
        return lazyCells;
    }

    public static List<Cell> getCells(final Iterable<Element> elements, final ElementSerialisation serialisation) throws SerialisationException {
        final List<Cell> cells = new ArrayList<>();
        for (final Element element : elements) {
            final Pair<Cell> cellPair = getCells(element, serialisation);
            cells.add(cellPair.getFirst());
            if (null != cellPair.getSecond()) {
                cells.add(cellPair.getSecond());
            }
        }

        return cells;
    }

    public static Pair<Cell> getCells(final Element element, final ElementSerialisation serialisation) throws SerialisationException {
        return getCells(serialisation.getPuts(element));
    }

    public static Pair<Cell> getCells(final Pair<Put> puts) {
        final Pair<Cell> cells = new Pair<>();
        cells.setFirst(puts.getFirst().getFamilyCellMap().values().iterator().next().iterator().next());

        if (null != puts.getSecond()) {
            cells.setSecond(puts.getSecond().getFamilyCellMap().values().iterator().next().iterator().next());
        }

        return cells;
    }
}
