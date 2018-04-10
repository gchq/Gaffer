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

package uk.gov.gchq.gaffer.hbasestore.coprocessor.processor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

import uk.gov.gchq.gaffer.commonutil.ByteUtil;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.serialisation.LazyElementCell;
import uk.gov.gchq.gaffer.hbasestore.utils.HBaseStoreConstants;
import uk.gov.gchq.koryphe.Since;

@Since("1.0.0")
public class ElementDedupeFilterProcessor extends FilterProcessor {
    // This element serialisation does not have the schema so not all methods can be used.
    private final ElementSerialisation elementSerialisation = new ElementSerialisation(null);

    private final boolean edges;
    private final boolean entities;
    private final boolean unDirectedEdges;
    private final boolean directedEdges;

    public ElementDedupeFilterProcessor(final boolean entities, final boolean edges, final DirectedType directedType) {
        this.entities = entities;
        this.edges = edges;
        directedEdges = DirectedType.DIRECTED == directedType;
        unDirectedEdges = DirectedType.UNDIRECTED == directedType;
    }

    @Override
    public boolean test(final LazyElementCell elementCell) {
        final Cell cell = elementCell.getCell();
        final byte flag = getFlag(cell);
        final boolean isEdge = flag != HBaseStoreConstants.ENTITY;

        if (!edges && isEdge) {
            return false;
        }

        if (!entities && !isEdge) {
            return false;
        }

        return !isEdge || testEdge(flag, cell);
    }

    public boolean isEdges() {
        return edges;
    }

    public boolean isEntities() {
        return entities;
    }

    public boolean isUnDirectedEdges() {
        return unDirectedEdges;
    }

    public boolean isDirectedEdges() {
        return directedEdges;
    }

    private byte getFlag(final Cell cell) {
        final byte[] rowID = CellUtil.cloneRow(cell);
        return rowID[rowID.length - 1];
    }

    private boolean testEdge(final byte flag, final Cell cell) {
        final boolean isUndirected = flag == HBaseStoreConstants.UNDIRECTED_EDGE;
        if (unDirectedEdges) {
            // Only undirected edges
            return isUndirected && testForDuplicateUndirectedEdge(cell);
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
            elementSerialisation.getSourceAndDestination(CellUtil.cloneRow(cell), sourceDestValues);
        } catch (final SerialisationException e) {
            throw new RuntimeException("Unable to deserialise element source and destination");
        }
        return ByteUtil.compareSortedBytes(sourceDestValues[0], sourceDestValues[1]) <= 0;
    }

    private boolean testDirection(final byte flag) {
        return flag != HBaseStoreConstants.INCORRECT_WAY_DIRECTED_EDGE;
    }
}
