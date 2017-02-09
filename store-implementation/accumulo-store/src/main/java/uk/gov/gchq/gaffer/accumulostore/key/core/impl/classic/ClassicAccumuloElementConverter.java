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

package uk.gov.gchq.gaffer.accumulostore.key.core.impl.classic;

import org.apache.accumulo.core.data.Key;
import uk.gov.gchq.gaffer.accumulostore.key.core.AbstractCoreKeyAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.Arrays;
import java.util.Map;

public class ClassicAccumuloElementConverter extends AbstractCoreKeyAccumuloElementConverter {
    public ClassicAccumuloElementConverter(final Schema schema) {
        super(schema);
    }

    @Override
    protected byte[] getRowKeyFromEntity(final Entity entity) throws AccumuloElementConversionException {
        // No Delimiters but need to escape bytes
        // because later we check how many delimiter characters there are
        try {
            return ByteArrayEscapeUtils.escape(getVertexSerialiser().serialise(entity.getVertex()));
        } catch (final SerialisationException e) {
            throw new AccumuloElementConversionException("Failed to serialise Entity Identifier", e);
        }
    }

    @Override
    protected Pair<byte[]> getRowKeysFromEdge(final Edge edge) throws AccumuloElementConversionException {
        // If edge is undirected then direction flag is 0 for both keys.
        // If edge is directed then for the first key the direction flag is 1
        // indicating that the first identifier in the key is the source
        // and the second is the destination, and for the second key
        // the direction flag is 2 indicating that the first identifier
        // in the key is the destination and the second identifier
        // in the key is the source, i.e. they need flipping around.
        byte directionFlag1;
        byte directionFlag2;
        if (edge.isDirected()) {
            directionFlag1 = ClassicBytePositions.CORRECT_WAY_DIRECTED_EDGE;
            directionFlag2 = ClassicBytePositions.INCORRECT_WAY_DIRECTED_EDGE;
        } else {
            directionFlag1 = ClassicBytePositions.UNDIRECTED_EDGE;
            directionFlag2 = ClassicBytePositions.UNDIRECTED_EDGE;
        }

        // Serialise source and destination to byte arrays, escaping if necessary
        final byte[] source = getSerialisedSource(edge);
        final byte[] destination = getSerialisedDestination(edge);

        // Length of row key is the length of the source plus the length of the destination
        // plus one for the delimiter in between the source and destination
        // plus one for the delimiter in between the destination and the direction flag
        // plus one for the direction flag at the end.
        final int length = source.length + destination.length + 3;
        final byte[] rowKey1 = new byte[length];
        final byte[] rowKey2 = new byte[length];

        // Create first key: source DELIMITER destination
        // DELIMITER (CORRECT_WAY_DIRECTED_EDGE or UNDIRECTED_EDGE)
        System.arraycopy(source, 0, rowKey1, 0, source.length);
        rowKey1[source.length] = ByteArrayEscapeUtils.DELIMITER;
        System.arraycopy(destination, 0, rowKey1, source.length + 1, destination.length);
        rowKey1[rowKey1.length - 2] = ByteArrayEscapeUtils.DELIMITER;
        rowKey1[rowKey1.length - 1] = directionFlag1;

        // Create second key: destination DELIMITER source
        // DELIMITER (INCORRECT_WAY_DIRECTED_EDGE or UNDIRECTED_EDGE)
        System.arraycopy(destination, 0, rowKey2, 0, destination.length);
        rowKey2[destination.length] = ByteArrayEscapeUtils.DELIMITER;
        System.arraycopy(source, 0, rowKey2, destination.length + 1, source.length);
        rowKey2[rowKey2.length - 2] = ByteArrayEscapeUtils.DELIMITER;
        rowKey2[rowKey2.length - 1] = directionFlag2;

        // Is this a self-edge? If so then return null for the second rowKey as
        // we don't want the same edge to go into Accumulo twice.
        if (selfEdge(edge)) {
            return new Pair<>(rowKey1, null);
        }
        return new Pair<>(rowKey1, rowKey2);
    }

    @Override
    protected boolean doesKeyRepresentEntity(final byte[] row) throws AccumuloElementConversionException {
        short numDelims = 0;
        for (final byte rowPart : row) {
            if (rowPart == ByteArrayEscapeUtils.DELIMITER) {
                numDelims++;
            }
        }

        if (numDelims != 0 && numDelims != 2) {
            throw new AccumuloElementConversionException(
                    "Wrong number of delimiters found in row key - found " + numDelims + ", expected 0 or 2.");
        }

        return numDelims == 0;
    }

    @Override
    protected Entity getEntityFromKey(final Key key) throws AccumuloElementConversionException {
        try {
            final Entity entity = new Entity(getGroupFromKey((key)), getVertexSerialiser()
                    .deserialise(ByteArrayEscapeUtils.unEscape(key.getRowData().getBackingArray())));
            addPropertiesToElement(entity, key);
            return entity;
        } catch (final SerialisationException e) {
            throw new AccumuloElementConversionException("Failed to re-create Entity from key", e);
        }
    }

    @Override
    protected boolean getSourceAndDestinationFromRowKey(final byte[] rowKey, final byte[][] sourceDestValue,
                                                        final Map<String, String> options) throws AccumuloElementConversionException {
        // Get sourceValue, destinationValue and directed flag from row key
        // Expect to find 2 delimiters (3 fields)
        final int[] positionsOfDelimiters = new int[2];
        short numDelims = 0;
        for (int i = 0; i < rowKey.length; i++) {
            if (rowKey[i] == ByteArrayEscapeUtils.DELIMITER) {
                if (numDelims >= 2) {
                    throw new AccumuloElementConversionException(
                            "Too many delimiters found in row key - found more than the expected 2.");
                }
                positionsOfDelimiters[numDelims++] = i;
            }
        }
        if (numDelims != 2) {
            throw new AccumuloElementConversionException(
                    "Wrong number of delimiters found in row key - found " + numDelims + ", expected 2.");
        }
        // If edge is undirected then create edge
        // (no need to worry about which direction the vertices should go in).
        // If the edge is directed then need to decide which way round the vertices should go.
        final int directionFlag = rowKey[rowKey.length - 1];
        if (directionFlag == ClassicBytePositions.UNDIRECTED_EDGE) {
            // Edge is undirected
            sourceDestValue[0] = getSourceBytes(rowKey, positionsOfDelimiters);
            sourceDestValue[1] = getDestBytes(rowKey, positionsOfDelimiters);
            return false;
        } else if (directionFlag == ClassicBytePositions.CORRECT_WAY_DIRECTED_EDGE) {
            // Edge is directed and the first identifier is the source of the edge
            sourceDestValue[0] = getSourceBytes(rowKey, positionsOfDelimiters);
            sourceDestValue[1] = getDestBytes(rowKey, positionsOfDelimiters);
            return true;
        } else if (directionFlag == ClassicBytePositions.INCORRECT_WAY_DIRECTED_EDGE) {
            // Edge is directed and the second identifier is the source of the edge
            int src = 1;
            int dst = 0;
            if (matchEdgeSource(options)) {
                src = 0;
                dst = 1;
            }
            sourceDestValue[src] = getSourceBytes(rowKey, positionsOfDelimiters);
            sourceDestValue[dst] = getDestBytes(rowKey, positionsOfDelimiters);
            return true;
        } else {
            throw new AccumuloElementConversionException(
                    "Invalid direction flag in row key - flag was " + directionFlag);
        }
    }

    private byte[] getDestBytes(final byte[] rowKey, final int[] positionsOfDelimiters) {
        return ByteArrayEscapeUtils
                .unEscape(Arrays.copyOfRange(rowKey, positionsOfDelimiters[0] + 1, positionsOfDelimiters[1]));
    }

    private byte[] getSourceBytes(final byte[] rowKey, final int[] positionsOfDelimiters) {
        return ByteArrayEscapeUtils
                .unEscape(Arrays.copyOfRange(rowKey, 0, positionsOfDelimiters[0]));
    }

    private boolean matchEdgeSource(final Map<String, String> options) {
        return options != null
                && options.containsKey(AccumuloStoreConstants.OPERATION_RETURN_MATCHED_SEEDS_AS_EDGE_SOURCE)
                && "true".equalsIgnoreCase(options.get(AccumuloStoreConstants.OPERATION_RETURN_MATCHED_SEEDS_AS_EDGE_SOURCE));
    }
}
