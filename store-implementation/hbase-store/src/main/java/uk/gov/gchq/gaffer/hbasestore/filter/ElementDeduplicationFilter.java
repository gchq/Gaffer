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
import java.io.IOException;

public class ElementDeduplicationFilter extends FilterBase {
    protected static final JSONSerialiser JSON_SERIALISER = new JSONSerialiser();

    // This element serialisation does not have the schema so not all  methods can be used.
    private ElementSerialisation elementSerialisation = new ElementSerialisation(null);
    private ElementDeduplicationFilterProperties properties;

    public ElementDeduplicationFilter() {
        this(new ElementDeduplicationFilterProperties());
    }

    public ElementDeduplicationFilter(final ElementDeduplicationFilterProperties properties) {
        this.properties = properties;
    }

    public static ElementDeduplicationFilter parseFrom(final byte[] pbBytes) throws DeserializationException {
        final ElementDeduplicationFilterProperties props;
        try {
            props = JSON_SERIALISER.deserialise(pbBytes, ElementDeduplicationFilterProperties.class);
        } catch (SerialisationException e) {
            throw new DeserializationException(e);
        }

        return new ElementDeduplicationFilter(props);
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
            return isUndirected && (!properties.isDeduplicateUndirectedEdges() || checkForDuplicateUndirectedEdge(cell));
        }

        if (properties.isDirectedEdges()) {
            // Only directed edges
            return !isUndirected && checkDirection(flag);
        }

        // All edge types
        if (isUndirected && properties.isDeduplicateUndirectedEdges()) {
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
        if (properties.isIncomingEdges()) {
            if (flag == ByteEntityPositions.CORRECT_WAY_DIRECTED_EDGE) {
                return false;
            }
        } else if (properties.isOutgoingEdges()) {
            if (flag == ByteEntityPositions.INCORRECT_WAY_DIRECTED_EDGE) {
                return false;
            }
        }
        return true;
    }
}
