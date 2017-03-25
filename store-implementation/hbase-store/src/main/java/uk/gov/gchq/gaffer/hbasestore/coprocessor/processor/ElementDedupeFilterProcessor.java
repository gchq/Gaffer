/*
 * Copyright 2016-2017 Crown Copyright
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
import org.apache.hadoop.hbase.CellUtil;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.serialisation.LazyElementCell;
import uk.gov.gchq.gaffer.hbasestore.utils.ByteEntityPositions;
import uk.gov.gchq.gaffer.hbasestore.utils.ByteUtils;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters.DirectedType;

public class ElementDedupeFilterProcessor extends FilterProcessor {
    // This element serialisation does not have the schema so not all methods can be used.
    private final ElementSerialisation elementSerialisation = new ElementSerialisation(null);

    private final boolean edges;
    private final boolean entities;
    private final boolean unDirectedEdges;
    private final boolean directedEdges;

    public ElementDedupeFilterProcessor(final View view, final DirectedType directedType) {
        entities = view.hasEntities();
        edges = view.hasEdges();
        directedEdges = DirectedType.DIRECTED == directedType;
        unDirectedEdges = DirectedType.UNDIRECTED == directedType;
    }

    @Override
    public boolean test(final LazyElementCell elementCell) {
        final Cell cell = elementCell.getCell();
        final byte flag = getFlag(cell);
        final boolean isEdge = flag != ByteEntityPositions.ENTITY;

        if (!edges && isEdge) {
            return false;
        }

        if (!entities && !isEdge) {
            return false;
        }

        return !isEdge || testEdge(flag, cell);
    }

    private byte getFlag(final Cell cell) {
        final byte[] rowID = CellUtil.cloneRow(cell);
        return rowID[rowID.length - 1];
    }

    private boolean testEdge(final byte flag, final Cell cell) {
        final boolean isUndirected = flag == ByteEntityPositions.UNDIRECTED_EDGE;
        if (unDirectedEdges) {
            // Only undirected edges
            return isUndirected && (testForDuplicateUndirectedEdge(cell));
        }

        if (directedEdges) {
            // Only directed edges
            return !isUndirected && testDirection(flag);
        }

        // All edge types
        if (isUndirected) {
            return testForDuplicateUndirectedEdge(cell);
        }

        return testDirection(flag);
    }

    private boolean testForDuplicateUndirectedEdge(final Cell cell) {
        final byte[][] sourceDestValues = new byte[3][];
        try {
            elementSerialisation.getSourceAndDestination(CellUtil.cloneRow(cell), sourceDestValues, null);
        } catch (SerialisationException e) {
            throw new RuntimeException("Unable to deserialise element source and destination");
        }
        return ByteUtils.compareBytes(sourceDestValues[0], sourceDestValues[1]) <= 0;
    }

    private boolean testDirection(final byte flag) {
        return flag != ByteEntityPositions.INCORRECT_WAY_DIRECTED_EDGE;
    }
}
