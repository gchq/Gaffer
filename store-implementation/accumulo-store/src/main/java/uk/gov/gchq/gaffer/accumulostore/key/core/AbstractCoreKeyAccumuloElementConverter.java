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
package uk.gov.gchq.gaffer.accumulostore.key.core;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialisation;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawSerialisationUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

@SuppressWarnings("unchecked")
public abstract class AbstractCoreKeyAccumuloElementConverter implements AccumuloElementConverter {
    protected final Schema schema;

    public AbstractCoreKeyAccumuloElementConverter(final Schema schema) {
        this.schema = schema;
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    @Override
    public Pair<Key> getKeysFromElement(final Element element) throws AccumuloElementConversionException {
        if (element instanceof Entity) {
            final Key key = getKeyFromEntity((Entity) element);
            return new Pair<>(key, null);
        }

        return getKeysFromEdge((Edge) element);
    }

    @Override
    public Pair<Key> getKeysFromEdge(final Edge edge) throws AccumuloElementConversionException {
        // Get pair of row keys
        final Pair<byte[]> rowKeys = getRowKeysFromEdge(edge);
        final byte[] columnFamily = buildColumnFamily(edge.getGroup());
        final byte[] columnQualifier = buildColumnQualifier(edge.getGroup(), edge.getProperties());
        final byte[] columnVisibility = buildColumnVisibility(edge.getGroup(), edge.getProperties());
        final long timeStamp = buildTimestamp(edge.getProperties());
        // Create Accumulo keys - note that second row key may be null (if it's
        // a self-edge) and
        // in that case we should return null second key
        final Key key1 = new Key(rowKeys.getFirst(), columnFamily, columnQualifier, columnVisibility, timeStamp);
        final Key key2 = rowKeys.getSecond() != null
                ? new Key(rowKeys.getSecond(), columnFamily, columnQualifier, columnVisibility, timeStamp) : null;
        // Return pair of keys
        return new Pair<>(key1, key2);
    }

    @Override
    public Key getKeyFromEntity(final Entity entity) throws AccumuloElementConversionException {
        // Row key is formed from vertex
        final byte[] rowKey = getRowKeyFromEntity(entity);
        final byte[] columnFamily = buildColumnFamily(entity.getGroup());
        final byte[] columnQualifier = buildColumnQualifier(entity.getGroup(), entity.getProperties());

        // Column visibility is formed from the visibility
        final byte[] columnVisibility = buildColumnVisibility(entity.getGroup(), entity.getProperties());

        final long timeStamp = buildTimestamp(entity.getProperties());

        // Create and return key
        return new Key(rowKey, columnFamily, columnQualifier, columnVisibility, timeStamp);
    }

    @Override
    public Value getValueFromProperties(final String group, final Properties properties)
            throws AccumuloElementConversionException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new AccumuloElementConversionException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }
        final Iterator<String> propertyNames = elementDefinition.getProperties().iterator();
        String propertyName;
        while (propertyNames.hasNext()) {
            propertyName = propertyNames.next();
            final TypeDefinition typeDefinition = elementDefinition.getPropertyTypeDef(propertyName);
            if (isStoredInValue(propertyName, elementDefinition)) {
                final Serialisation serialiser = (typeDefinition != null) ? typeDefinition.getSerialiser() : null;
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
                        writeBytes(AccumuloStoreConstants.EMPTY_BYTES, out);
                    }
                } catch (final IOException e) {
                    throw new AccumuloElementConversionException("Failed to write serialise property to ByteArrayOutputStream" + propertyName, e);
                }
            }
        }

        return new Value(out.toByteArray());
    }

    @Override
    public Value getValueFromElement(final Element element) throws AccumuloElementConversionException {
        return getValueFromProperties(element.getGroup(), element.getProperties());
    }

    @Override
    public Properties getPropertiesFromValue(final String group, final Value value)
            throws AccumuloElementConversionException {
        final Properties properties = new Properties();
        if (value == null || value.getSize() == 0) {
            return properties;
        }
        final byte[] bytes = value.get();
        int lastDelimiter = 0;
        final int arrayLength = bytes.length;
        long currentPropLength;
        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new AccumuloElementConversionException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }
        final Iterator<String> propertyNames = elementDefinition.getProperties().iterator();
        while (propertyNames.hasNext() && lastDelimiter < arrayLength) {
            final String propertyName = propertyNames.next();
            if (isStoredInValue(propertyName, elementDefinition)) {
                final TypeDefinition typeDefinition = elementDefinition.getPropertyTypeDef(propertyName);
                final Serialisation<?> serialiser = (typeDefinition != null) ? typeDefinition.getSerialiser() : null;
                if (null != serialiser) {
                    final int numBytesForLength = CompactRawSerialisationUtils.decodeVIntSize(bytes[lastDelimiter]);
                    final byte[] length = new byte[numBytesForLength];
                    System.arraycopy(bytes, lastDelimiter, length, 0, numBytesForLength);
                    try {
                        currentPropLength = CompactRawSerialisationUtils.readLong(length);
                    } catch (final SerialisationException e) {
                        throw new AccumuloElementConversionException("Exception reading length of property", e);
                    }
                    lastDelimiter += numBytesForLength;
                    if (currentPropLength > 0) {
                        try {
                            properties.put(propertyName, serialiser.deserialise(Arrays.copyOfRange(bytes, lastDelimiter, lastDelimiter += currentPropLength)));
                        } catch (SerialisationException e) {
                            throw new AccumuloElementConversionException("Failed to deserialise property " + propertyName, e);
                        }
                    } else {
                        try {
                            properties.put(propertyName, serialiser.deserialiseEmptyBytes());
                        } catch (SerialisationException e) {
                            throw new AccumuloElementConversionException("Failed to deserialise property " + propertyName, e);
                        }
                    }
                }
            }
        }

        return properties;
    }

    @Override
    public Element getElementFromKey(final Key key) throws AccumuloElementConversionException {
        return getElementFromKey(key, null);
    }

    @Override
    public Element getElementFromKey(final Key key, final Map<String, String> options)
            throws AccumuloElementConversionException {
        final boolean keyRepresentsEntity = doesKeyRepresentEntity(key.getRowData().getBackingArray());
        if (keyRepresentsEntity) {
            return getEntityFromKey(key);
        }
        return getEdgeFromKey(key, options);
    }

    @Override
    public Element getFullElement(final Key key, final Value value) throws AccumuloElementConversionException {
        return getFullElement(key, value, null);
    }

    @Override
    public Element getFullElement(final Key key, final Value value, final Map<String, String> options)
            throws AccumuloElementConversionException {
        final Element element = getElementFromKey(key, options);
        element.copyProperties(getPropertiesFromValue(element.getGroup(), value));
        return element;
    }

    @Override
    public byte[] buildColumnFamily(final String group) throws AccumuloElementConversionException {
        try {
            return group.getBytes(CommonConstants.UTF_8);
        } catch (final UnsupportedEncodingException e) {
            throw new AccumuloElementConversionException(e.getMessage(), e);
        }
    }

    @Override
    public String getGroupFromColumnFamily(final byte[] columnFamily) throws AccumuloElementConversionException {
        try {
            return new String(columnFamily, CommonConstants.UTF_8);
        } catch (final UnsupportedEncodingException e) {
            throw new AccumuloElementConversionException(e.getMessage(), e);
        }
    }

    @Override
    public byte[] buildColumnVisibility(final String group, final Properties properties)
            throws AccumuloElementConversionException {
        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new AccumuloElementConversionException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }
        if (null != schema.getVisibilityProperty()) {
            final TypeDefinition propertyDef = elementDefinition.getPropertyTypeDef(schema.getVisibilityProperty());
            if (null != propertyDef) {
                final Object property = properties.get(schema.getVisibilityProperty());
                if (property != null) {
                    try {
                        return propertyDef.getSerialiser().serialise(property);
                    } catch (final SerialisationException e) {
                        throw new AccumuloElementConversionException(e.getMessage(), e);
                    }
                } else {
                    return propertyDef.getSerialiser().serialiseNull();
                }
            }
        }

        return AccumuloStoreConstants.EMPTY_BYTES;
    }

    @Override
    public Properties getPropertiesFromColumnVisibility(final String group, final byte[] columnVisibility)
            throws AccumuloElementConversionException {
        final Properties properties = new Properties();

        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new AccumuloElementConversionException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }

        if (null != schema.getVisibilityProperty()) {
            final TypeDefinition propertyDef = elementDefinition.getPropertyTypeDef(schema.getVisibilityProperty());
            if (null != propertyDef) {
                final Serialisation serialiser = propertyDef.getSerialiser();
                try {
                    if (columnVisibility == null || columnVisibility.length == 0) {
                        final Object value = serialiser.deserialiseEmptyBytes();
                        if (value != null) {
                            properties.put(schema.getVisibilityProperty(), value);
                        }
                    } else {
                        properties.put(schema.getVisibilityProperty(),
                                serialiser.deserialise(columnVisibility));
                    }
                } catch (final SerialisationException e) {
                    throw new AccumuloElementConversionException(e.getMessage(), e);
                }
            }
        }

        return properties;
    }

    @Override
    public byte[] buildColumnQualifier(final String group, final Properties properties)
            throws AccumuloElementConversionException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new AccumuloElementConversionException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }
        final Iterator<String> propertyNames = elementDefinition.getGroupBy().iterator();
        while (propertyNames.hasNext()) {
            String propertyName = propertyNames.next();
            final TypeDefinition typeDefinition = elementDefinition.getPropertyTypeDef(propertyName);
            final Serialisation serialiser = (typeDefinition != null) ? typeDefinition.getSerialiser() : null;
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
                    writeBytes(AccumuloStoreConstants.EMPTY_BYTES, out);
                }
            } catch (final IOException e) {
                throw new AccumuloElementConversionException("Failed to write serialise property to ByteArrayOutputStream" + propertyName, e);
            }
        }

        return out.toByteArray();
    }

    @Override
    public Properties getPropertiesFromColumnQualifier(final String group, final byte[] bytes)
            throws AccumuloElementConversionException {
        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new AccumuloElementConversionException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }

        final Properties properties = new Properties();
        if (bytes == null || bytes.length == 0) {
            return properties;
        }

        int lastDelimiter = 0;
        final int arrayLength = bytes.length;
        long currentPropLength;
        final Iterator<String> propertyNames = elementDefinition.getGroupBy().iterator();
        while (propertyNames.hasNext() && lastDelimiter < arrayLength) {
            final String propertyName = propertyNames.next();
            final TypeDefinition typeDefinition = elementDefinition.getPropertyTypeDef(propertyName);
            final Serialisation<?> serialiser = (typeDefinition != null) ? typeDefinition.getSerialiser() : null;
            if (null != serialiser) {
                final int numBytesForLength = CompactRawSerialisationUtils.decodeVIntSize(bytes[lastDelimiter]);
                final byte[] length = new byte[numBytesForLength];
                System.arraycopy(bytes, lastDelimiter, length, 0, numBytesForLength);
                try {
                    currentPropLength = CompactRawSerialisationUtils.readLong(length);
                } catch (final SerialisationException e) {
                    throw new AccumuloElementConversionException("Exception reading length of property", e);
                }
                lastDelimiter += numBytesForLength;
                if (currentPropLength > 0) {
                    try {
                        properties.put(propertyName, serialiser.deserialise(Arrays.copyOfRange(bytes, lastDelimiter, lastDelimiter += currentPropLength)));
                    } catch (SerialisationException e) {
                        throw new AccumuloElementConversionException("Failed to deserialise property " + propertyName, e);
                    }
                }
            }
        }

        return properties;
    }

    @Override
    public byte[] getPropertiesAsBytesFromColumnQualifier(final String group, final byte[] bytes, final int numProps)
            throws AccumuloElementConversionException {
        if (numProps == 0 || bytes == null || bytes.length == 0) {
            return AccumuloStoreConstants.EMPTY_BYTES;
        }
        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new AccumuloElementConversionException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }
        if (numProps == elementDefinition.getProperties().size()) {
            return bytes;
        }
        int lastDelimiter = 0;
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
                throw new AccumuloElementConversionException("Exception reading length of property", e);
            }

            lastDelimiter += numBytesForLength;
            if (currentPropLength > 0) {
                lastDelimiter += currentPropLength;
            }

            propIndex++;
        }

        final byte[] propertyBytes = new byte[lastDelimiter];
        System.arraycopy(bytes, 0, propertyBytes, 0, lastDelimiter);
        return propertyBytes;
    }

    @Override
    public long buildTimestamp(final Properties properties) throws AccumuloElementConversionException {
        if (null != schema.getTimestampProperty()) {
            final Object property = properties.get(schema.getTimestampProperty());
            if (property == null) {
                return System.currentTimeMillis();
            } else {
                return (Long) property;
            }
        }
        return System.currentTimeMillis();
    }

    /**
     * Get the properties for a given group defined in the Schema as being
     * stored in the Accumulo timestamp column.
     *
     * @param group     The {@link Element} type to be queried
     * @param timestamp the element timestamp property
     * @return The Properties stored within the Timestamp part of the
     * {@link Key}
     * @throws AccumuloElementConversionException If the supplied group has not been defined
     */
    @Override
    public Properties getPropertiesFromTimestamp(final String group, final long timestamp)
            throws AccumuloElementConversionException {
        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new AccumuloElementConversionException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }

        final Properties properties = new Properties();
        // If the element group requires a timestamp property then add it.
        if (null != schema.getTimestampProperty() && elementDefinition.containsProperty(schema.getTimestampProperty())) {
            properties.put(schema.getTimestampProperty(), timestamp);
        }
        return properties;
    }

    @Override
    public byte[] serialiseVertex(final Object vertex) throws AccumuloElementConversionException {
        try {
            return ByteArrayEscapeUtils.escape(this.schema.getVertexSerialiser().serialise(vertex));
        } catch (final SerialisationException e) {
            throw new AccumuloElementConversionException(
                    "Failed to serialise given identifier object for use in the bloom filter", e);
        }
    }

    protected abstract byte[] getRowKeyFromEntity(final Entity entity) throws AccumuloElementConversionException;

    protected abstract Pair<byte[]> getRowKeysFromEdge(final Edge edge) throws AccumuloElementConversionException;

    protected abstract boolean doesKeyRepresentEntity(final byte[] row) throws AccumuloElementConversionException;

    protected abstract Entity getEntityFromKey(final Key key) throws AccumuloElementConversionException;

    protected abstract boolean getSourceAndDestinationFromRowKey(final byte[] rowKey,
                                                                 final byte[][] sourceValueDestinationValue, final Map<String, String> options)
            throws AccumuloElementConversionException;

    protected boolean selfEdge(final Edge edge) {
        return edge.getSource().equals(edge.getDestination());
    }

    protected void addPropertiesToElement(final Element element, final Key key)
            throws AccumuloElementConversionException {
        element.copyProperties(
                getPropertiesFromColumnQualifier(element.getGroup(), key.getColumnQualifierData().getBackingArray()));
        element.copyProperties(
                getPropertiesFromColumnVisibility(element.getGroup(), key.getColumnVisibilityData().getBackingArray()));
        element.copyProperties(
                getPropertiesFromTimestamp(element.getGroup(), key.getTimestamp()));
    }

    protected Serialisation getVertexSerialiser() {
        return schema.getVertexSerialiser();
    }

    protected Edge getEdgeFromKey(final Key key, final Map<String, String> options)
            throws AccumuloElementConversionException {
        final byte[][] result = new byte[3][];
        final boolean directed = getSourceAndDestinationFromRowKey(key.getRowData().getBackingArray(), result, options);
        String group;
        try {
            group = new String(key.getColumnFamilyData().getBackingArray(), CommonConstants.UTF_8);
        } catch (final UnsupportedEncodingException e) {
            throw new AccumuloElementConversionException(e.getMessage(), e);
        }
        try {
            final Edge edge = new Edge(group, getVertexSerialiser().deserialise(result[0]),
                    getVertexSerialiser().deserialise(result[1]), directed);
            addPropertiesToElement(edge, key);
            return edge;
        } catch (final SerialisationException e) {
            throw new AccumuloElementConversionException("Failed to re-create Edge from key", e);
        }
    }

    protected byte[] getSerialisedSource(final Edge edge) throws AccumuloElementConversionException {
        try {
            return ByteArrayEscapeUtils.escape(getVertexSerialiser().serialise(edge.getSource()));
        } catch (final SerialisationException e) {
            throw new AccumuloElementConversionException("Failed to serialise Edge Source", e);
        }
    }

    protected byte[] getSerialisedDestination(final Edge edge) throws AccumuloElementConversionException {
        try {
            return ByteArrayEscapeUtils.escape(getVertexSerialiser().serialise(edge.getDestination()));
        } catch (final SerialisationException e) {
            throw new AccumuloElementConversionException("Failed to serialise Edge Destination", e);
        }
    }

    protected String getGroupFromKey(final Key key) throws AccumuloElementConversionException {
        try {
            return new String(key.getColumnFamilyData().getBackingArray(), CommonConstants.UTF_8);
        } catch (final UnsupportedEncodingException e) {
            throw new AccumuloElementConversionException("Failed to get element group from key", e);
        }
    }

    protected boolean isStoredInValue(final String propertyName, final SchemaElementDefinition elementDef) {
        return !elementDef.getGroupBy().contains(propertyName)
                && !propertyName.equals(schema.getVisibilityProperty())
                && !propertyName.equals(schema.getTimestampProperty());
    }

    private void writeBytes(final byte[] bytes, final ByteArrayOutputStream out)
            throws IOException {
        CompactRawSerialisationUtils.write(bytes.length, out);
        out.write(bytes);
    }

}
