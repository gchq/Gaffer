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

package uk.gov.gchq.gaffer.hbasestore.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.utils.ByteEntityPositions;
import uk.gov.gchq.gaffer.hbasestore.utils.ByteUtils;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters.DirectedType;
import java.io.IOException;

public class ElementDedupeFilter extends FilterBase {
    protected static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();

    // This element serialisation does not have the schema so not all methods can be used.
    private ElementSerialisation elementSerialisation = new ElementSerialisation(null);
    private ElementDedupeFilterProperties properties;

    public ElementDedupeFilter() {
        this(new ElementDedupeFilterProperties());
    }

    public ElementDedupeFilter(final ElementDedupeFilterProperties properties) {
        this.properties = properties;
    }

    public ElementDedupeFilter(final GraphFilters op) {
        this(new ElementDedupeFilterProperties(op));
    }

    public static ElementDedupeFilter parseFrom(final byte[] bytes) throws DeserializationException {
        final ElementDedupeFilterProperties props;
        try {
            props = JSON_SERIALISER.deserialise(bytes, ElementDedupeFilterProperties.class);
        } catch (SerialisationException e) {
            throw new DeserializationException(e);
        }

        return new ElementDedupeFilter(props);
    }

    @Override
    public ReturnCode filterKeyValue(final Cell cell) throws IOException {
        final byte flag = getFlag(cell);
        final boolean isEdge = flag != ByteEntityPositions.ENTITY;

        if (!properties.isEdges() && isEdge) {
            return ReturnCode.SKIP;
        }

        if (!properties.isEntities() && !isEdge) {
            return ReturnCode.SKIP;
        }

        if (!isEdge || checkEdge(flag, cell)) {
            return ReturnCode.INCLUDE;
        }

        return ReturnCode.SKIP;
    }


    @Override
    public byte[] toByteArray() throws IOException {
        return JSON_SERIALISER.serialise(properties);
    }

    private byte getFlag(final Cell cell) {
        final byte[] rowID = CellUtil.cloneRow(cell);
        return rowID[rowID.length - 1];
    }

    private boolean checkEdge(final byte flag, final Cell cell) throws SerialisationException {
        final boolean isUndirected = flag == ByteEntityPositions.UNDIRECTED_EDGE;
        if (properties.isUnDirectedEdges()) {
            // Only undirected edges
            return isUndirected && (checkForDuplicateUndirectedEdge(cell));
        }

        if (properties.isDirectedEdges()) {
            // Only directed edges
            return !isUndirected && checkDirection(flag);
        }

        // All edge types
        if (isUndirected) {
            return checkForDuplicateUndirectedEdge(cell);
        }

        return checkDirection(flag);
    }

    private boolean checkForDuplicateUndirectedEdge(final Cell cell) throws SerialisationException {
        final byte[][] sourceDestValues = new byte[3][];
        elementSerialisation.getSourceAndDestination(CellUtil.cloneRow(cell), sourceDestValues, null);
        return ByteUtils.compareBytes(sourceDestValues[0], sourceDestValues[1]) <= 0;
    }

    private boolean checkDirection(final byte flag) {
        return flag != ByteEntityPositions.INCORRECT_WAY_DIRECTED_EDGE;
    }

    public static class ElementDedupeFilterProperties {
        private boolean edges = false;
        private boolean entities = false;
        private boolean unDirectedEdges = false;
        private boolean directedEdges = false;

        public ElementDedupeFilterProperties() {
        }

        public ElementDedupeFilterProperties(final GraphFilters op) {
            entities = op.getView().hasEntities();
            edges = op.getView().hasEdges();
            directedEdges = DirectedType.DIRECTED == op.getDirectedType();
            unDirectedEdges = DirectedType.UNDIRECTED == op.getDirectedType();
        }

        public boolean isEdges() {
            return edges;
        }

        public void setEdges(final boolean edges) {
            this.edges = edges;
        }

        public boolean isEntities() {
            return entities;
        }

        public void setEntities(final boolean entities) {
            this.entities = entities;
        }

        public boolean isUnDirectedEdges() {
            return unDirectedEdges;
        }

        public void setUnDirectedEdges(final boolean unDirectedEdges) {
            this.unDirectedEdges = unDirectedEdges;
        }

        public boolean isDirectedEdges() {
            return directedEdges;
        }

        public void setDirectedEdges(final boolean directedEdges) {
            this.directedEdges = directedEdges;
        }
    }
}
