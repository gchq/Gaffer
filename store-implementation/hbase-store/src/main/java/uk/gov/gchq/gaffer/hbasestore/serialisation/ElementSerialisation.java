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

package uk.gov.gchq.gaffer.hbasestore.serialisation;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.EdgeDirection;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hbasestore.utils.HBaseStoreConstants;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawSerialisationUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;

public class ElementSerialisation {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElementSerialisation.class);

    private final Schema schema;

    public ElementSerialisation(final Schema schema) {
        this.schema = schema;
    }

    public byte[] getValue(final Element element) throws SerialisationException {
        return getValue(element.getGroup(), element.getProperties());
    }

    public byte[] getValue(final String group, final Properties properties)
            throws SerialisationException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new SerialisationException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }
        final Iterator<String> propertyNames = elementDefinition.getProperties().iterator();
        String propertyName;
        while (propertyNames.hasNext()) {
            propertyName = propertyNames.next();
            final TypeDefinition typeDefinition = elementDefinition.getPropertyTypeDef(propertyName);
            if (isStoredInValue(propertyName, elementDefinition)) {
                final ToBytesSerialiser serialiser = (null != typeDefinition) ? (ToBytesSerialiser) typeDefinition.getSerialiser() : null;
                try {
                    if (null != serialiser) {
                        Object value = properties.get(propertyName);
                        if (null != value) {
                            final byte[] bytes = serialiser.serialise(value);
                            writeBytes(bytes, out);
                        } else {
                            final byte[] bytes = serialiser.serialiseNull();
                            writeBytes(bytes, out);
                        }
                    } else {
                        writeBytes(HBaseStoreConstants.EMPTY_BYTES, out);
                    }
                } catch (final IOException e) {
                    throw new SerialisationException("Failed to write serialise property to ByteArrayOutputStream" + propertyName, e);
                }
            }
        }

        return out.toByteArray();
    }

    public Properties getPropertiesFromValue(final String group, final byte[] value)
            throws SerialisationException {
        final Properties properties = new Properties();
        if (null == value || value.length == 0) {
            return properties;
        }
        int lastDelimiter = 0;
        final int arrayLength = value.length;
        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new SerialisationException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }
        final Iterator<String> propertyNames = elementDefinition.getProperties().iterator();
        while (propertyNames.hasNext() && lastDelimiter < arrayLength) {
            final String propertyName = propertyNames.next();
            if (isStoredInValue(propertyName, elementDefinition)) {
                final TypeDefinition typeDefinition = elementDefinition.getPropertyTypeDef(propertyName);
                final ToBytesSerialiser serialiser = (null != typeDefinition) ? (ToBytesSerialiser) typeDefinition.getSerialiser() : null;
                if (null != serialiser) {
                    final int numBytesForLength = CompactRawSerialisationUtils.decodeVIntSize(value[lastDelimiter]);
                    int currentPropLength;
                    try {
                        // value is never larger than int.
                        currentPropLength = (int) CompactRawSerialisationUtils.readLong(value, lastDelimiter);
                    } catch (final SerialisationException e) {
                        throw new SerialisationException("Exception reading length of property");
                    }
                    lastDelimiter += numBytesForLength;
                    if (currentPropLength > 0) {
                        try {
                            properties.put(propertyName, serialiser.deserialise(value, lastDelimiter, currentPropLength));
                            lastDelimiter += currentPropLength;
                        } catch (final SerialisationException e) {
                            throw new SerialisationException("Failed to deserialise property " + propertyName, e);
                        }
                    } else {
                        try {
                            properties.put(propertyName, serialiser.deserialiseEmpty());
                        } catch (final SerialisationException e) {
                            throw new SerialisationException("Failed to deserialise property " + propertyName, e);
                        }
                    }
                } else {
                    LOGGER.warn("No serialiser found in schema for property {} in group {}", propertyName, group);
                }
            }
        }

        return properties;
    }

    public Element getPartialElement(final String group, final byte[] rowId, final boolean includeMatchedVertex) throws SerialisationException {
        return getElement(CellUtil.createCell(rowId, HBaseStoreConstants.getColFam(), getColumnQualifier(group, new Properties())), includeMatchedVertex);
    }

    public Element getElement(final Cell cell, final boolean includeMatchedVertex)
            throws SerialisationException {
        final boolean keyRepresentsEntity = isEntity(cell);
        if (keyRepresentsEntity) {
            return getEntity(cell);
        }
        return getEdge(cell, includeMatchedVertex);
    }

    public byte[] getColumnVisibility(final Element element) throws SerialisationException {
        return getColumnVisibility(element.getGroup(), element.getProperties());
    }

    public byte[] getColumnVisibility(final String group, final Properties properties)
            throws SerialisationException {
        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new SerialisationException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }
        if (null != schema.getVisibilityProperty()) {
            final TypeDefinition propertyDef = elementDefinition.getPropertyTypeDef(schema.getVisibilityProperty());
            if (null != propertyDef) {
                final Object property = properties.get(schema.getVisibilityProperty());
                final ToBytesSerialiser serialiser = (ToBytesSerialiser) propertyDef.getSerialiser();
                if (null != property) {
                    try {
                        return serialiser.serialise(property);
                    } catch (final SerialisationException e) {
                        throw new SerialisationException(e.getMessage(), e);
                    }
                } else {
                    return serialiser.serialiseNull();
                }
            }
        }

        return HBaseStoreConstants.EMPTY_BYTES;
    }

    public byte[] getColumnQualifier(final Element element) throws SerialisationException {
        return getColumnQualifier(element.getGroup(), element.getProperties());
    }

    public byte[] getColumnQualifier(final String group, final Properties properties)
            throws SerialisationException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new SerialisationException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }

        try {
            final byte[] groupBytes = Bytes.toBytes(group);
            writeBytes(groupBytes, out);
        } catch (final IOException e) {
            throw new SerialisationException("Failed to serialise group to ByteArrayOutputStream", e);
        }

        for (final String propertyName : elementDefinition.getGroupBy()) {
            final TypeDefinition typeDefinition = elementDefinition.getPropertyTypeDef(propertyName);
            final ToBytesSerialiser serialiser = (null != typeDefinition) ? (ToBytesSerialiser) typeDefinition.getSerialiser() : null;
            try {
                if (null != serialiser) {
                    Object value = properties.get(propertyName);
                    if (null != value) {
                        final byte[] bytes = serialiser.serialise(value);
                        writeBytes(bytes, out);
                    } else {
                        final byte[] bytes = serialiser.serialiseNull();
                        writeBytes(bytes, out);
                    }
                } else {
                    writeBytes(HBaseStoreConstants.EMPTY_BYTES, out);
                }
            } catch (final IOException e) {
                throw new SerialisationException("Failed to write serialise property to ByteArrayOutputStream" + propertyName, e);
            }
        }

        return out.toByteArray();
    }

    public Properties getPropertiesFromColumnQualifier(final String group, final byte[] bytes)
            throws SerialisationException {
        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new SerialisationException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }

        final Properties properties = new Properties();
        if (null == bytes || bytes.length == 0) {
            return properties;
        }

        int carriage = CompactRawSerialisationUtils.decodeVIntSize(bytes[0]) + Bytes.toBytes(group).length;
        final int arrayLength = bytes.length;

        final Iterator<String> propertyNames = elementDefinition.getGroupBy().iterator();
        while (propertyNames.hasNext() && carriage < arrayLength) {
            final String propertyName = propertyNames.next();
            final TypeDefinition typeDefinition = elementDefinition.getPropertyTypeDef(propertyName);
            final ToBytesSerialiser serialiser = (null != typeDefinition) ? (ToBytesSerialiser) typeDefinition.getSerialiser() : null;
            if (null != serialiser) {
                final int numBytesForLength = CompactRawSerialisationUtils.decodeVIntSize(bytes[carriage]);
                int currentPropLength;
                try {
                    //this value is never greater than int.
                    currentPropLength = (int) CompactRawSerialisationUtils.readLong(bytes, carriage);
                } catch (final SerialisationException e) {
                    throw new SerialisationException("Exception reading length of property");
                }
                carriage += numBytesForLength;
                if (currentPropLength > 0) {
                    try {
                        properties.put(propertyName, serialiser.deserialise(bytes, carriage, currentPropLength));
                        carriage += currentPropLength;
                    } catch (final SerialisationException e) {
                        throw new SerialisationException("Failed to deserialise property " + propertyName, e);
                    }
                } else {
                    try {
                        properties.put(propertyName, serialiser.deserialiseEmpty());
                    } catch (final SerialisationException e) {
                        throw new SerialisationException("Failed to deserialise property " + propertyName, e);
                    }
                }
            }
        }

        return properties;
    }

    public byte[] getPropertiesAsBytesFromColumnQualifier(final String group, final byte[] bytes, final int numProps)
            throws SerialisationException {
        if (numProps == 0 || null == bytes || bytes.length == 0) {
            return HBaseStoreConstants.EMPTY_BYTES;
        }
        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new SerialisationException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }

        final int firstDelimiter = CompactRawSerialisationUtils.decodeVIntSize(bytes[0]) + Bytes.toBytes(group).length;
        if (numProps == elementDefinition.getProperties().size()) {
            final int length = bytes.length - firstDelimiter;
            final byte[] propertyBytes = new byte[length];
            System.arraycopy(bytes, firstDelimiter, propertyBytes, 0, length);
            return propertyBytes;
        }
        int lastDelimiter = firstDelimiter;
        final int arrayLength = bytes.length;
        long currentPropLength;
        int propIndex = 0;
        while (propIndex < numProps && lastDelimiter < arrayLength) {
            final int numBytesForLength = CompactRawSerialisationUtils.decodeVIntSize(bytes[lastDelimiter]);
            final byte[] length = new byte[numBytesForLength];
            System.arraycopy(bytes, lastDelimiter, length, 0, numBytesForLength);
            try {
                currentPropLength = CompactRawSerialisationUtils.readLong(length);
            } catch (final SerialisationException e) {
                throw new SerialisationException("Exception reading length of property");
            }

            lastDelimiter += numBytesForLength;
            if (currentPropLength > 0) {
                lastDelimiter += currentPropLength;
            }

            propIndex++;
        }

        final int length = lastDelimiter - firstDelimiter;
        final byte[] propertyBytes = new byte[length];
        System.arraycopy(bytes, firstDelimiter, propertyBytes, 0, length);
        return propertyBytes;
    }

    public long getTimestamp(final Element element) throws SerialisationException {
        return getTimestamp(element.getProperties());
    }

    public long getTimestamp(final Properties properties) throws SerialisationException {
        if (null != schema.getTimestampProperty()) {
            final Object property = properties.get(schema.getTimestampProperty());
            if (null == property) {
                return System.currentTimeMillis();
            } else {
                return (Long) property;
            }
        }
        return System.currentTimeMillis();
    }

    /**
     * Get the properties for a given group defined in the Schema as being
     * stored in the HBase timestamp column.
     *
     * @param group     The {@link Element} type to be queried
     * @param timestamp the element timestamp property
     * @return The Properties stored within the Timestamp part of the
     * {@link Cell}
     * @throws SerialisationException If the supplied group has not been defined
     */

    public Properties getPropertiesFromTimestamp(final String group, final long timestamp)
            throws SerialisationException {
        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new SerialisationException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }

        final Properties properties = new Properties();
        // If the element group requires a timestamp property then add it.
        if (null != schema.getTimestampProperty() && elementDefinition.containsProperty(schema.getTimestampProperty())) {
            properties.put(schema.getTimestampProperty(), timestamp);
        }
        return properties;
    }

    public byte[] serialiseVertex(final Object vertex) throws SerialisationException {
        try {
            return ByteArrayEscapeUtils.escape(((ToBytesSerialiser) schema.getVertexSerialiser()).serialise(vertex));
        } catch (final SerialisationException e) {
            throw new SerialisationException("Failed to serialise given vertex object.", e);
        }
    }

    public boolean isEntity(final Cell cell) throws SerialisationException {
        final byte[] row = CellUtil.cloneRow(cell);
        return row[row.length - 1] == HBaseStoreConstants.ENTITY;
    }


    public String getGroup(final Cell cell) throws SerialisationException {
        return getGroup(CellUtil.cloneQualifier(cell));
    }

    public String getGroup(final byte[] columnQualifier) throws SerialisationException {
        try {
            final int numBytesForLength = CompactRawSerialisationUtils.decodeVIntSize(columnQualifier[0]);
            int currentPropLength = (int) CompactRawSerialisationUtils.readLong(columnQualifier, 0);
            return new String(columnQualifier, numBytesForLength, currentPropLength, Charset.forName("UTF-8"));
        } catch (final SerialisationException e) {
            throw new SerialisationException("Exception reading length of property");
        }
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    public Pair<byte[], byte[]> getRowKeys(final Element element) throws SerialisationException {
        if (element instanceof Entity) {
            return new Pair<>(getRowKey((Entity) element));
        }

        return getRowKeys((Edge) element);
    }

    public byte[] getRowKey(final Entity entity) throws SerialisationException {
        byte[] value;
        try {
            value = serialiseVertex(entity.getVertex());
            final byte[] returnVal = Arrays.copyOf(value, value.length + 2);
            returnVal[returnVal.length - 2] = ByteArrayEscapeUtils.DELIMITER;
            returnVal[returnVal.length - 1] = HBaseStoreConstants.ENTITY;
            return returnVal;
        } catch (final SerialisationException e) {
            throw new SerialisationException("Failed to serialise Entity Identifier", e);
        }
    }

    public Pair<byte[], byte[]> getRowKeys(final Edge edge) throws SerialisationException {
        byte directionFlag1;
        byte directionFlag2;
        if (edge.isDirected()) {
            directionFlag1 = HBaseStoreConstants.CORRECT_WAY_DIRECTED_EDGE;
            directionFlag2 = HBaseStoreConstants.INCORRECT_WAY_DIRECTED_EDGE;
        } else {
            directionFlag1 = HBaseStoreConstants.UNDIRECTED_EDGE;
            directionFlag2 = HBaseStoreConstants.UNDIRECTED_EDGE;
        }
        final byte[] source = serialiseVertex(edge.getSource());
        final byte[] destination = serialiseVertex(edge.getDestination());
        final int length = source.length + destination.length + 5;
        final byte[] rowKey1 = new byte[length];
        System.arraycopy(source, 0, rowKey1, 0, source.length);
        rowKey1[source.length] = ByteArrayEscapeUtils.DELIMITER;
        rowKey1[source.length + 1] = directionFlag1;
        rowKey1[source.length + 2] = ByteArrayEscapeUtils.DELIMITER;
        System.arraycopy(destination, 0, rowKey1, source.length + 3, destination.length);
        rowKey1[rowKey1.length - 2] = ByteArrayEscapeUtils.DELIMITER;
        rowKey1[rowKey1.length - 1] = directionFlag1;
        if (edge.getSource().equals(edge.getDestination())) {
            return new Pair<>(rowKey1, null);
        }

        final byte[] rowKey2 = new byte[length];
        System.arraycopy(destination, 0, rowKey2, 0, destination.length);
        rowKey2[destination.length] = ByteArrayEscapeUtils.DELIMITER;
        rowKey2[destination.length + 1] = directionFlag2;
        rowKey2[destination.length + 2] = ByteArrayEscapeUtils.DELIMITER;
        System.arraycopy(source, 0, rowKey2, destination.length + 3, source.length);
        rowKey2[rowKey2.length - 2] = ByteArrayEscapeUtils.DELIMITER;
        rowKey2[rowKey2.length - 1] = directionFlag2;
        return new Pair<>(rowKey1, rowKey2);
    }

    public Pair<Put, Put> getPuts(final Element element) throws SerialisationException {
        final Pair<byte[], byte[]> row = getRowKeys(element);
        final byte[] cq = getColumnQualifier(element);
        return getPuts(element, row, cq);
    }

    public Pair<Put, Put> getPuts(final Element element, final Pair<byte[], byte[]> row, final byte[] cq) throws SerialisationException {
        final long ts = getTimestamp(element.getProperties());
        final byte[] value = getValue(element);
        final String visibilityStr = Bytes.toString(getColumnVisibility(element));
        final CellVisibility cellVisibility = visibilityStr.isEmpty() ? null : new CellVisibility(visibilityStr);
        final Put put = new Put(row.getFirst());
        put.addColumn(HBaseStoreConstants.getColFam(), cq, ts, value);
        if (null != cellVisibility) {
            put.setCellVisibility(cellVisibility);
        }

        final Pair<Put, Put> puts = new Pair<>(put);
        if (null != row.getSecond()) {
            final Put put2 = new Put(row.getSecond());
            put2.addColumn(HBaseStoreConstants.getColFam(), cq, value);
            if (null != cellVisibility) {
                put2.setCellVisibility(cellVisibility);
            }
            puts.setSecond(put2);
        }

        return puts;
    }

    public EdgeDirection getSourceAndDestination(final byte[] rowKey, final byte[][] sourceDestValues) throws SerialisationException {
        // Get element class, sourceValue, destinationValue and directed flag from row cell
        // Expect to find 3 delimiters (4 fields)
        final int[] positionsOfDelimiters = new int[3];
        short numDelims = 0;
        // Last byte will be directional flag so don't count it
        for (int i = 0; i < rowKey.length - 1; ++i) {
            if (rowKey[i] == ByteArrayEscapeUtils.DELIMITER) {
                if (numDelims >= 3) {
                    throw new SerialisationException(
                            "Too many delimiters found in row cell - found more than the expected 3.");
                }
                positionsOfDelimiters[numDelims++] = i;
            }
        }
        if (numDelims != 3) {
            throw new SerialisationException(
                    "Wrong number of delimiters found in row cell - found " + numDelims + ", expected 3.");
        }
        // If edge is undirected then create edge
        // (no need to worry about which direction the vertices should go in).
        // If the edge is directed then need to decide which way round the vertices should go.
        byte directionFlag;
        try {
            directionFlag = rowKey[rowKey.length - 1];
        } catch (final NumberFormatException e) {
            throw new SerialisationException("Error parsing direction flag from row cell - " + e);
        }
        byte[] sourceBytes = ByteArrayEscapeUtils.unEscape(rowKey, 0, positionsOfDelimiters[0]);
        byte[] destBytes = ByteArrayEscapeUtils.unEscape(rowKey, positionsOfDelimiters[1] + 1, positionsOfDelimiters[2]);
        sourceDestValues[0] = sourceBytes;
        sourceDestValues[1] = destBytes;
        EdgeDirection rtn;
        switch (directionFlag) {
            case HBaseStoreConstants.UNDIRECTED_EDGE:
                // Edge is undirected
                rtn = EdgeDirection.UNDIRECTED;
                break;
            case HBaseStoreConstants.CORRECT_WAY_DIRECTED_EDGE:
                // Edge is directed and the first identifier is the source of the edge
                rtn = EdgeDirection.DIRECTED;
                break;
            case HBaseStoreConstants.INCORRECT_WAY_DIRECTED_EDGE:
                // Edge is directed and the second identifier is the source of the edge
                sourceDestValues[0] = destBytes;
                sourceDestValues[1] = sourceBytes;
                rtn = EdgeDirection.DIRECTED_REVERSED;
                break;
            default:
                throw new SerialisationException(
                        "Invalid direction flag in row cell - flag was " + directionFlag);
        }
        return rtn;
    }

    private boolean isStoredInValue(final String propertyName, final SchemaElementDefinition elementDef) {
        return !elementDef.getGroupBy().contains(propertyName)
                && !propertyName.equals(schema.getTimestampProperty());
    }

    private void writeBytes(final byte[] bytes, final ByteArrayOutputStream out)
            throws IOException {
        CompactRawSerialisationUtils.write(bytes.length, out);
        out.write(bytes);
    }

    private void addPropertiesToElement(final Element element, final Cell cell)
            throws SerialisationException {
        element.copyProperties(
                getPropertiesFromColumnQualifier(element.getGroup(), CellUtil.cloneQualifier(cell)));
        element.copyProperties(
                getPropertiesFromValue(element.getGroup(), CellUtil.cloneValue(cell)));
        element.copyProperties(
                getPropertiesFromTimestamp(element.getGroup(), cell.getTimestamp()));
    }

    private Edge getEdge(final Cell cell)
            throws SerialisationException {
        return getEdge(cell, false);
    }

    private Edge getEdge(final Cell cell, final boolean includeMatchedVertex)
            throws SerialisationException {
        final byte[][] result = new byte[3][];
        final EdgeDirection direction = getSourceAndDestination(CellUtil.cloneRow(cell), result);
        final EdgeId.MatchedVertex matchedVertex;
        if (!includeMatchedVertex) {
            matchedVertex = null;
        } else if (EdgeDirection.DIRECTED_REVERSED == direction) {
            matchedVertex = EdgeId.MatchedVertex.DESTINATION;
        } else {
            matchedVertex = EdgeId.MatchedVertex.SOURCE;
        }
        final String group = getGroup(cell);
        try {
            final Edge edge = new Edge(group, ((ToBytesSerialiser) schema.getVertexSerialiser()).deserialise(result[0]),
                    ((ToBytesSerialiser) schema.getVertexSerialiser()).deserialise(result[1]), direction.isDirected(), matchedVertex, null);
            addPropertiesToElement(edge, cell);
            return edge;
        } catch (final SerialisationException e) {
            throw new SerialisationException("Failed to re-create Edge from cell", e);
        }
    }

    private Entity getEntity(final Cell cell) throws SerialisationException {

        try {
            final byte[] row = CellUtil.cloneRow(cell);
            final Entity entity = new Entity(getGroup(cell), ((ToBytesSerialiser) schema.getVertexSerialiser())
                    .deserialise(ByteArrayEscapeUtils.unEscape(row, 0, row.length - 2)));
            addPropertiesToElement(entity, cell);
            return entity;
        } catch (final SerialisationException e) {
            throw new SerialisationException("Failed to re-create Entity from cell", e);
        }
    }
}
