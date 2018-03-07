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

package uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity;

import org.apache.accumulo.core.data.Key;

import uk.gov.gchq.gaffer.accumulostore.key.core.AbstractCoreKeyAccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.EdgeDirection;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Arrays;

/**
 * The ByteEntityAccumuloElementConverter converts Gaffer Elements to Accumulo
 * Keys and Values.
 * <p>
 * The way keys are created can be summarised as the following. For Edges the
 * resulting key will be: Source Value + Delimiter + Flag + Delimiter +
 * Destination Value + Delimiter + Flag, and a second edge of Destination Value
 * + Delimiter + Flag + Delimiter + Source Value + Delimiter + Flag).
 * <p>
 * For entities the resulting key will be: Identifier Value + Delimiter + Flag
 * <p>
 * Note that the Delimiter referenced in the above example is the byte
 * representation of the number 0 for this implementation and the values are
 * appropriately escaped. The Flag is a byte value that changes depending on
 * whether it being used on an entity, an undirected edge or a directed edge
 * input as the user specified or reversed. The flag values are as follows:
 * Entity = 1, Undirected Edge = 4, Directed Edge = 2, Reversed Directed Edge = 3.
 */
public class ByteEntityAccumuloElementConverter extends AbstractCoreKeyAccumuloElementConverter {

    public ByteEntityAccumuloElementConverter(final Schema schema) {
        super(schema);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected EntityId getEntityId(final byte[] row) {
        try {
            return new EntitySeed(((ToBytesSerialiser) schema.getVertexSerialiser())
                    .deserialise(ByteArrayEscapeUtils.unEscape(Arrays.copyOfRange(row, 0,
                            row.length - 2))));
        } catch (final SerialisationException e) {
            throw new AccumuloElementConversionException("Failed to create EntityId from Accumulo row key", e);
        }
    }

    @Override
    protected byte[] getRowKeyFromEntity(final Entity entity) {
        try {
            return ByteArrayEscapeUtils.escape(((ToBytesSerialiser) schema.getVertexSerialiser()).serialise(entity.getVertex()),
                    ByteArrayEscapeUtils.DELIMITER,
                    ByteEntityPositions.ENTITY);

        } catch (final SerialisationException e) {
            throw new AccumuloElementConversionException("Failed to serialise Entity Identifier", e);
        }
    }

    @Override
    protected Pair<byte[], byte[]> getRowKeysFromEdge(final Edge edge) {
        byte[] source = getSerialisedSource(edge);
        byte[] destination = getSerialisedDestination(edge);

        byte directionFlag = edge.isDirected() ? ByteEntityPositions.CORRECT_WAY_DIRECTED_EDGE : ByteEntityPositions.UNDIRECTED_EDGE;
        final byte[] rowKey1 = getRowKey(source, destination, directionFlag);

        byte[] rowKey2 = null;
        if (!selfEdge(edge)) {
            byte invertDirectedFlag = (directionFlag == ByteEntityPositions.CORRECT_WAY_DIRECTED_EDGE) ? ByteEntityPositions.INCORRECT_WAY_DIRECTED_EDGE : directionFlag;
            rowKey2 = getRowKey(destination, source, invertDirectedFlag);
        }

        return new Pair<>(rowKey1, rowKey2);
    }

    private byte[] getRowKey(final byte[] first, final byte[] second, final byte directionFlag) {
        int carriage = first.length;
        int secondLen = second.length;
        byte[] rowKey = new byte[carriage + secondLen + 5];
        System.arraycopy(first, 0, rowKey, 0, carriage);
        rowKey[carriage++] = ByteArrayEscapeUtils.DELIMITER;
        rowKey[carriage++] = directionFlag;
        rowKey[carriage++] = ByteArrayEscapeUtils.DELIMITER;
        System.arraycopy(second, 0, rowKey, carriage, secondLen);
        carriage += secondLen;
        rowKey[carriage++] = ByteArrayEscapeUtils.DELIMITER;
        rowKey[carriage] = directionFlag;
        return rowKey;
    }

    @Override
    protected boolean doesKeyRepresentEntity(final byte[] row) {
        return row[row.length - 1] == ByteEntityPositions.ENTITY;
    }

    @Override
    protected Entity getEntityFromKey(final Key key, final byte[] row) {
        try {
            final Entity entity = new Entity(getGroupFromKey(key), ((ToBytesSerialiser) schema.getVertexSerialiser())
                    .deserialise(ByteArrayEscapeUtils.unEscape(row, 0, row.length - 2)));
            addPropertiesToElement(entity, key);
            return entity;
        } catch (final SerialisationException e) {
            throw new AccumuloElementConversionException("Failed to re-create Entity from key", e);
        }
    }

    @Override
    protected EdgeDirection getSourceAndDestinationFromRowKey(final byte[] rowKey, final byte[][] sourceDestValues) {
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

        byte[] sourceBytes = ByteArrayEscapeUtils.unEscape(rowKey, 0, positionsOfDelimiters[0]);
        byte[] destBytes = ByteArrayEscapeUtils.unEscape(rowKey, positionsOfDelimiters[1] + 1, positionsOfDelimiters[2]);
        EdgeDirection rtn;
        sourceDestValues[0] = sourceBytes;
        sourceDestValues[1] = destBytes;

        switch (directionFlag) {
            case ByteEntityPositions.UNDIRECTED_EDGE:
                // Edge is undirected
                rtn = EdgeDirection.UNDIRECTED;
                break;
            case ByteEntityPositions.CORRECT_WAY_DIRECTED_EDGE:
                // Edge is directed and the first identifier is the source of the edge
                rtn = EdgeDirection.DIRECTED;
                break;
            case ByteEntityPositions.INCORRECT_WAY_DIRECTED_EDGE:
                // Edge is directed and the second identifier is the source of the edge
                sourceDestValues[0] = destBytes;
                sourceDestValues[1] = sourceBytes;
                rtn = EdgeDirection.DIRECTED_REVERSED;
                break;
            default:
                throw new AccumuloElementConversionException(
                        "Invalid direction flag in row key - flag was " + directionFlag);
        }
        return rtn;
    }
}
