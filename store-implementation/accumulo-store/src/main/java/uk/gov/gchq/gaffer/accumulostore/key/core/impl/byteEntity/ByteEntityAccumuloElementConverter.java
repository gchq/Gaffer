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

package uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity;

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

/**
 * The ByteEntityAccumuloElementConverter converts Gaffer Elements to Accumulo
 * Keys And Values
 * <p>
 * The way keys are created can be summarised as the following. For Edges the
 * resulting key will be: Source Value + Delimiter + Flag + Delimiter +
 * Destination Value + Delimiter + Flag (And a second edge of Destination Value
 * + Delimiter + Flag + Delimiter + Source Value + Delimiter + Flag for
 * searching)
 * <p>
 * For entities the resulting key will be: Identifier Value + Delimiter + Flag
 * <p>
 * Note that the Delimiter referenced in the above example is the byte
 * representation of the number 0 for this implementation and the values are
 * appropriately escaped. And the Flag is a byte value that changes depending on
 * whether it being used on an entity, an undirected edge and a directed edge
 * input as the user specified or as the one input inverted for searching. The
 * flag values are as follows: Entity = 1 Undirected Edge = 4 Directed Edge = 2
 * Inverted Directed Edge = 3
 * <p>
 * Values are constructed by placing all the properties in a map of Property
 * Name : Byte Value
 * <p>
 * And then serialising the entire map to bytes.
 */
public class ByteEntityAccumuloElementConverter extends AbstractCoreKeyAccumuloElementConverter {

    public ByteEntityAccumuloElementConverter(final Schema schema) {
        super(schema);
    }

    @Override
    protected byte[] getRowKeyFromEntity(final Entity entity) throws AccumuloElementConversionException {
        byte[] value;
        try {
            value = ByteArrayEscapeUtils.escape(getVertexSerialiser().serialise(entity.getVertex()));
            final byte[] returnVal = Arrays.copyOf(value, value.length + 2);
            returnVal[returnVal.length - 2] = ByteArrayEscapeUtils.DELIMITER;
            returnVal[returnVal.length - 1] = ByteEntityPositions.ENTITY;
            return returnVal;
        } catch (final SerialisationException e) {
            throw new AccumuloElementConversionException("Failed to serialise Entity Identifier", e);
        }
    }

    @Override
    protected Pair<byte[]> getRowKeysFromEdge(final Edge edge) throws AccumuloElementConversionException {
        byte directionFlag1;
        byte directionFlag2;
        if (edge.isDirected()) {
            directionFlag1 = ByteEntityPositions.CORRECT_WAY_DIRECTED_EDGE;
            directionFlag2 = ByteEntityPositions.INCORRECT_WAY_DIRECTED_EDGE;
        } else {
            directionFlag1 = ByteEntityPositions.UNDIRECTED_EDGE;
            directionFlag2 = ByteEntityPositions.UNDIRECTED_EDGE;
        }
        final byte[] source = getSerialisedSource(edge);
        final byte[] destination = getSerialisedDestination(edge);

        final int length = source.length + destination.length + 5;
        final byte[] rowKey1 = new byte[length];
        System.arraycopy(source, 0, rowKey1, 0, source.length);
        rowKey1[source.length] = ByteArrayEscapeUtils.DELIMITER;
        rowKey1[source.length + 1] = directionFlag1;
        rowKey1[source.length + 2] = ByteArrayEscapeUtils.DELIMITER;
        System.arraycopy(destination, 0, rowKey1, source.length + 3, destination.length);
        rowKey1[rowKey1.length - 2] = ByteArrayEscapeUtils.DELIMITER;
        rowKey1[rowKey1.length - 1] = directionFlag1;
        final byte[] rowKey2 = new byte[length];
        System.arraycopy(destination, 0, rowKey2, 0, destination.length);
        rowKey2[destination.length] = ByteArrayEscapeUtils.DELIMITER;
        rowKey2[destination.length + 1] = directionFlag2;
        rowKey2[destination.length + 2] = ByteArrayEscapeUtils.DELIMITER;
        System.arraycopy(source, 0, rowKey2, destination.length + 3, source.length);
        rowKey2[rowKey2.length - 2] = ByteArrayEscapeUtils.DELIMITER;
        rowKey2[rowKey2.length - 1] = directionFlag2;
        if (selfEdge(edge)) {
            return new Pair<>(rowKey1, null);
        }
        return new Pair<>(rowKey1, rowKey2);
    }

    @Override
    protected boolean doesKeyRepresentEntity(final byte[] row) {
        return row[row.length - 1] == ByteEntityPositions.ENTITY;
    }

    @Override
    protected Entity getEntityFromKey(final Key key) throws AccumuloElementConversionException {
        try {
            final Entity entity = new Entity(getGroupFromKey(key), getVertexSerialiser()
                    .deserialise(ByteArrayEscapeUtils.unEscape(Arrays.copyOfRange(key.getRowData().getBackingArray(), 0,
                            (key.getRowData().getBackingArray().length) - 2))));
            addPropertiesToElement(entity, key);
            return entity;
        } catch (final SerialisationException e) {
            throw new AccumuloElementConversionException("Failed to re-create Entity from key", e);
        }
    }

    @Override
    protected boolean getSourceAndDestinationFromRowKey(final byte[] rowKey, final byte[][] sourceDestValues,
                                                        final Map<String, String> options) throws AccumuloElementConversionException {
        // Get element class, sourceValue, destinationValue and directed flag from row key
        // Expect to find 3 delimiters (4 fields)
        final int[] positionsOfDelimiters = new int[3];
        short numDelims = 0;
        // Last byte will be directional flag so don't count it
        for (int i = 0; i < rowKey.length - 1; ++i) {
            if (rowKey[i] == ByteArrayEscapeUtils.DELIMITER) {
                if (numDelims >= 3) {
                    throw new AccumuloElementConversionException(
                            "Too many delimiters found in row key - found more than the expected 3.");
                }
                positionsOfDelimiters[numDelims++] = i;
            }
        }
        if (numDelims != 3) {
            throw new AccumuloElementConversionException(
                    "Wrong number of delimiters found in row key - found " + numDelims + ", expected 3.");
        }
        // If edge is undirected then create edge
        // (no need to worry about which direction the vertices should go in).
        // If the edge is directed then need to decide which way round the vertices should go.
        byte directionFlag;
        try {
            directionFlag = rowKey[rowKey.length - 1];
        } catch (final NumberFormatException e) {
            throw new AccumuloElementConversionException("Error parsing direction flag from row key - " + e);
        }
        if (directionFlag == ByteEntityPositions.UNDIRECTED_EDGE) {
            // Edge is undirected
            sourceDestValues[0] = getSourceBytes(rowKey, positionsOfDelimiters);
            sourceDestValues[1] = getDestBytes(rowKey, positionsOfDelimiters);
            return false;
        } else if (directionFlag == ByteEntityPositions.CORRECT_WAY_DIRECTED_EDGE) {
            // Edge is directed and the first identifier is the source of the edge
            sourceDestValues[0] = getSourceBytes(rowKey, positionsOfDelimiters);
            sourceDestValues[1] = getDestBytes(rowKey, positionsOfDelimiters);
            return true;
        } else if (directionFlag == ByteEntityPositions.INCORRECT_WAY_DIRECTED_EDGE) {
            // Edge is directed and the second identifier is the source of the edge
            int src = 1;
            int dst = 0;
            if (matchEdgeSource(options)) {
                src = 0;
                dst = 1;
            }
            sourceDestValues[src] = getSourceBytes(rowKey, positionsOfDelimiters);
            sourceDestValues[dst] = getDestBytes(rowKey, positionsOfDelimiters);
            return true;
        } else {
            throw new AccumuloElementConversionException(
                    "Invalid direction flag in row key - flag was " + directionFlag);
        }
    }

    private byte[] getDestBytes(final byte[] rowKey, final int[] positionsOfDelimiters) {
        return ByteArrayEscapeUtils
                .unEscape(Arrays.copyOfRange(rowKey, positionsOfDelimiters[1] + 1, positionsOfDelimiters[2]));
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
