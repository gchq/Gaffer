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
import uk.gov.gchq.gaffer.accumulostore.utils.BytesAndRange;
import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.EdgeDirection;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawSerialisationUtils;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;

@SuppressWarnings("unchecked")
public abstract class AbstractCoreKeyAccumuloElementConverter implements AccumuloElementConverter {
    protected final Schema schema;

    public AbstractCoreKeyAccumuloElementConverter(final Schema schema) {
        this.schema = schema;
    }

    @Override
    public ElementId getElementId(final Key key, final boolean includeMatchedVertex) {
        final byte[] row = key.getRowData().getBackingArray();
        if (doesKeyRepresentEntity(row)) {
            return getEntityId(row);
        }
        return getEdgeId(row, includeMatchedVertex);
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    @Override
    public Pair<Key, Key> getKeysFromElement(final Element element) {
        if (element instanceof Entity) {
            final Key key = getKeyFromEntity((Entity) element);
            return new Pair<>(key, null);
        }

        return getKeysFromEdge((Edge) element);
    }

    @Override
    public Pair<Key, Key> getKeysFromEdge(final Edge edge) {
        // Get pair of row keys
        final Pair<byte[], byte[]> rowKeys = getRowKeysFromEdge(edge);
        final byte[] columnFamily = buildColumnFamily(edge.getGroup());
        final byte[] columnQualifier = buildColumnQualifier(edge.getGroup(), edge.getProperties());
        final byte[] columnVisibility = buildColumnVisibility(edge.getGroup(), edge.getProperties());
        final long timeStamp = buildTimestamp(edge.getProperties());
        // Create Accumulo keys - note that second row key may be null (if it's
        // a self-edge) and
        // in that case we should return null second key
        final Key key1 = new Key(rowKeys.getFirst(), columnFamily, columnQualifier, columnVisibility, timeStamp);
        final Key key2 = null != rowKeys.getSecond()
                ? new Key(rowKeys.getSecond(), columnFamily, columnQualifier, columnVisibility, timeStamp) : null;
        // Return pair of keys
        return new Pair<>(key1, key2);
    }

    @Override
    public Key getKeyFromEntity(final Entity entity) {
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

    @SuppressWarnings("Convert2streamapi")
    @Override
    public Value getValueFromProperties(final String group, final Properties properties) {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        final SchemaElementDefinition elementDefinition = getSchemaElementDefinition(group);

        for (final String propertyName : elementDefinition.getProperties()) {
            if (isStoredInValue(propertyName, elementDefinition)) {
                serialiseSizeAndPropertyValue(propertyName, elementDefinition, properties, stream);
            }
        }

        return new Value(stream.toByteArray());
    }

    @Override
    public Value getValueFromElement(final Element element) {
        return getValueFromProperties(element.getGroup(), element.getProperties());
    }

    @Override
    public Properties getPropertiesFromValue(final String group, final Value value) {
        final Properties properties = new Properties();
        if (isNotEmpty(value)) {
            final byte[] bytes = value.get();
            int delimiterPosition = 0;
            final int arrayLength = bytes.length;
            final SchemaElementDefinition elementDefinition = getSchemaElementDefinition(group);
            final Iterator<String> propertyNames = elementDefinition.getProperties().iterator();
            while (propertyNames.hasNext() && delimiterPosition < arrayLength) {
                final String propertyName = propertyNames.next();
                try {
                    if (isStoredInValue(propertyName, elementDefinition)) {
                        delimiterPosition = addDeserialisedProperty(bytes, delimiterPosition, properties, elementDefinition, propertyName);
                    }
                } catch (final SerialisationException e) {
                    throw new AccumuloElementConversionException("Failed to deserialise property " + propertyName, e);
                }
            }
        }
        return properties;
    }

    @Override
    public Element getElementFromKey(final Key key, final boolean includeMatchedVertex) {
        final byte[] row = key.getRowData().getBackingArray();
        final boolean keyRepresentsEntity = doesKeyRepresentEntity(row);
        if (keyRepresentsEntity) {
            return getEntityFromKey(key, row);
        }
        return getEdgeFromKey(key, row, includeMatchedVertex);
    }

    @Override
    public Element getFullElement(final Key key, final Value value, final boolean includeMatchedVertex) {
        final Element element = getElementFromKey(key, includeMatchedVertex);
        element.copyProperties(getPropertiesFromValue(element.getGroup(), value));
        return element;
    }

    @Override
    public Pair<byte[], byte[]> getRowKeysFromElement(final Element element) {
        if (element instanceof Entity) {
            final byte[] rowKey = getRowKeyFromEntity((Entity) element);
            return new Pair<>(rowKey, null);
        }

        return getRowKeysFromEdge((Edge) element);
    }

    @Override
    public byte[] buildColumnFamily(final String group) {
        try {
            return group.getBytes(CommonConstants.UTF_8);
        } catch (final UnsupportedEncodingException e) {
            throw new AccumuloElementConversionException(e.getMessage(), e);
        }
    }

    @Override
    public String getGroupFromColumnFamily(final byte[] columnFamily) {
        try {
            return new String(columnFamily, CommonConstants.UTF_8);
        } catch (final UnsupportedEncodingException e) {
            throw new AccumuloElementConversionException(e.getMessage(), e);
        }
    }

    @Override
    public byte[] buildColumnVisibility(final String group, final Properties properties) {
        byte[] rtn = AccumuloStoreConstants.EMPTY_BYTES;
        final SchemaElementDefinition elementDefinition = getSchemaElementDefinition(group);
        if (null != schema.getVisibilityProperty()) {
            final TypeDefinition propertyDef = elementDefinition.getPropertyTypeDef(schema.getVisibilityProperty());
            if (null != propertyDef) {
                final Object property = properties.get(schema.getVisibilityProperty());
                final ToBytesSerialiser serialiser = (ToBytesSerialiser) propertyDef.getSerialiser();
                if (null != property) {
                    try {
                        rtn = serialiser.serialise(property);
                    } catch (final SerialisationException e) {
                        throw new AccumuloElementConversionException(e.getMessage(), e);
                    }
                } else {
                    rtn = serialiser.serialiseNull();
                }
            }
        }
        return rtn;
    }

    @Override
    public Properties getPropertiesFromColumnVisibility(final String group, final byte[] columnVisibility) {
        final Properties properties = new Properties();

        final SchemaElementDefinition elementDefinition = getSchemaElementDefinition(group);

        if (null != schema.getVisibilityProperty()) {
            final TypeDefinition propertyDef = elementDefinition.getPropertyTypeDef(schema.getVisibilityProperty());
            if (null != propertyDef) {
                final ToBytesSerialiser serialiser = (ToBytesSerialiser) propertyDef.getSerialiser();
                try {
                    if (null == columnVisibility || columnVisibility.length == 0) {
                        final Object value = serialiser.deserialiseEmpty();
                        if (null != value) {
                            properties.put(schema.getVisibilityProperty(), value);
                        }
                    } else {
                        properties.put(schema.getVisibilityProperty(), serialiser.deserialise(columnVisibility));
                    }
                } catch (final SerialisationException e) {
                    throw new AccumuloElementConversionException(e.getMessage(), e);
                }
            }
        }

        return properties;
    }

    @Override
    public byte[] buildColumnQualifier(final String group, final Properties properties) {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        final SchemaElementDefinition elementDefinition = getSchemaElementDefinition(group);

        for (final String groupByPropertyName : elementDefinition.getGroupBy()) {
            serialiseSizeAndPropertyValue(groupByPropertyName, elementDefinition, properties, stream);
        }

        return stream.toByteArray();
    }

    private SchemaElementDefinition getSchemaElementDefinition(final String group) {
        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new AccumuloElementConversionException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }
        return elementDefinition;
    }

    protected void serialiseSizeAndPropertyValue(final String propertyName, final SchemaElementDefinition elementDefinition, final Properties properties, final ByteArrayOutputStream stream) {
        try {
            final TypeDefinition typeDefinition = elementDefinition.getPropertyTypeDef(propertyName);
            final ToBytesSerialiser serialiser = (null == typeDefinition) ? null : (ToBytesSerialiser) typeDefinition.getSerialiser();
            byte[] bytes;
            if (null == serialiser) {
                bytes = AccumuloStoreConstants.EMPTY_BYTES;
            } else {
                Object value = properties.get(propertyName);
                //serialiseNull could be different to AccumuloStoreConstants.EMPTY_BYTES
                bytes = (null == value) ? serialiser.serialiseNull() : serialiser.serialise(value);
            }
            writeBytes(bytes, stream);
        } catch (final IOException e) {
            throw new AccumuloElementConversionException("Failed to write serialised property to ByteArrayOutputStream" + propertyName, e);
        }
    }

    @Override
    public Properties getPropertiesFromColumnQualifier(final String group, final byte[] bytes) {
        final Properties properties = new Properties();
        if (null != bytes && bytes.length != 0) {
            int delimiterPosition = 0;
            final int arrayLength = bytes.length;
            final SchemaElementDefinition elementDefinition = getSchemaElementDefinition(group);
            final Iterator<String> propertyNames = elementDefinition.getGroupBy().iterator();
            while (propertyNames.hasNext() && delimiterPosition < arrayLength) {
                final String propertyName = propertyNames.next();
                try {
                    delimiterPosition = addDeserialisedProperty(bytes, delimiterPosition, properties, elementDefinition, propertyName);
                } catch (final SerialisationException e) {
                    throw new AccumuloElementConversionException("Failed to deserialise property " + propertyName, e);
                }
            }
        }
        return properties;
    }

    private int addDeserialisedProperty(final byte[] bytes, final int carriage, final Properties properties, final SchemaElementDefinition elementDefinition, final String propertyName) throws SerialisationException {
        int rtn = carriage;
        final TypeDefinition typeDefinition = elementDefinition.getPropertyTypeDef(propertyName);
        final ToBytesSerialiser serialiser = (null != typeDefinition) ? (ToBytesSerialiser) typeDefinition.getSerialiser() : null;
        if (null != serialiser) {
            final int numBytesForLength = CompactRawSerialisationUtils.decodeVIntSize(bytes[rtn]);
            final int currentPropLength = getCurrentPropLength(bytes, rtn);
            int from = rtn += numBytesForLength;
            rtn += currentPropLength;
            Object deserialisedObject = getDeserialisedObject(serialiser, bytes, from, currentPropLength);
            properties.put(propertyName, deserialisedObject);
        }
        return rtn;
    }

    @Override
    public BytesAndRange getPropertiesAsBytesFromColumnQualifier(final String group, final byte[] bytes, final int numProps) {
        BytesAndRange rtn = new BytesAndRange(bytes, 0, 0);
        if (isColumnQualifierBytesValid(bytes, numProps)) {
            final SchemaElementDefinition elementDefinition = getSchemaElementDefinition(group);
            if (numProps == elementDefinition.getProperties().size()) {
                rtn = new BytesAndRange(bytes, 0, bytes.length);
            } else {
                int delimiterPosition = 0;
                final int arrayLength = bytes.length;
                int propIndex = 0;
                while (propIndex < numProps && delimiterPosition < arrayLength) {
                    final int numBytesForLength = CompactRawSerialisationUtils.decodeVIntSize(bytes[delimiterPosition]);
                    final int currentPropLength = getCurrentPropLength(bytes, delimiterPosition);
                    delimiterPosition += currentPropLength + numBytesForLength;
                    propIndex++;
                }

                rtn = new BytesAndRange(bytes, 0, delimiterPosition);
            }
        }
        return rtn;
    }

    protected boolean isColumnQualifierBytesValid(final byte[] bytes, final int numProps) {
        return numProps != 0 && null != bytes && bytes.length != 0;
    }

    @Override
    public long buildTimestamp(final Properties properties) {
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
     * stored in the Accumulo timestamp column.
     *
     * @param group     The {@link Element} type to be queried
     * @param timestamp the element timestamp property
     * @return The Properties stored within the Timestamp part of the
     * {@link Key}
     */
    @Override
    public Properties getPropertiesFromTimestamp(final String group, final long timestamp) {
        final SchemaElementDefinition elementDefinition = getSchemaElementDefinition(group);

        final Properties properties = new Properties();
        // If the element group requires a timestamp property then add it.
        if (null != schema.getTimestampProperty() && elementDefinition.containsProperty(schema.getTimestampProperty())) {
            properties.put(schema.getTimestampProperty(), timestamp);
        }
        return properties;
    }

    @Override
    public byte[] serialiseVertex(final Object vertex) {
        try {
            return ByteArrayEscapeUtils.escape(((ToBytesSerialiser) schema.getVertexSerialiser()).serialise(vertex));
        } catch (final SerialisationException e) {
            throw new AccumuloElementConversionException(
                    "Failed to serialise given identifier object for use in the bloom filter", e);
        }
    }

    protected abstract byte[] getRowKeyFromEntity(final Entity entity);

    protected abstract Pair<byte[], byte[]> getRowKeysFromEdge(final Edge edge);

    protected abstract boolean doesKeyRepresentEntity(final byte[] row);

    protected abstract Entity getEntityFromKey(final Key key, final byte[] row);

    protected abstract EdgeDirection getSourceAndDestinationFromRowKey(final byte[] rowKey,
                                                                       final byte[][] sourceValueDestinationValue);

    protected abstract EntityId getEntityId(final byte[] row);

    protected EdgeId getEdgeId(final byte[] row, final boolean includeMatchedVertex) {
        final byte[][] result = new byte[2][];
        final EdgeDirection direction = getSourceAndDestinationFromRowKey(row, result);
        final EdgeId.MatchedVertex matchedVertex;
        if (!includeMatchedVertex) {
            matchedVertex = null;
        } else if (EdgeDirection.DIRECTED_REVERSED == direction) {
            matchedVertex = EdgeId.MatchedVertex.DESTINATION;
        } else {
            matchedVertex = EdgeId.MatchedVertex.SOURCE;
        }
        try {
            return new EdgeSeed(((ToBytesSerialiser) schema.getVertexSerialiser()).deserialise(result[0]),
                    ((ToBytesSerialiser) schema.getVertexSerialiser()).deserialise(result[1]), direction.isDirected(), matchedVertex);
        } catch (final SerialisationException e) {
            throw new AccumuloElementConversionException("Failed to create EdgeId from Accumulo row key", e);
        }
    }

    protected boolean selfEdge(final Edge edge) {
        return edge.getSource().equals(edge.getDestination());
    }

    protected void addPropertiesToElement(final Element element, final Key key) {
        element.copyProperties(
                getPropertiesFromColumnQualifier(element.getGroup(), key.getColumnQualifierData().getBackingArray()));
        element.copyProperties(
                getPropertiesFromColumnVisibility(element.getGroup(), key.getColumnVisibilityData().getBackingArray()));
        element.copyProperties(
                getPropertiesFromTimestamp(element.getGroup(), key.getTimestamp()));
    }

    @SuppressWarnings("WeakerAccess")
    protected Edge getEdgeFromKey(final Key key, final byte[] row, final boolean includeMatchedVertex) {
        final byte[][] result = new byte[2][];
        final EdgeDirection direction = getSourceAndDestinationFromRowKey(row, result);
        final EdgeId.MatchedVertex matchedVertex;
        if (!includeMatchedVertex) {
            matchedVertex = null;
        } else if (EdgeDirection.DIRECTED_REVERSED == direction) {
            matchedVertex = EdgeId.MatchedVertex.DESTINATION;
        } else {
            matchedVertex = EdgeId.MatchedVertex.SOURCE;
        }

        String group;
        try {
            group = new String(key.getColumnFamilyData().getBackingArray(), CommonConstants.UTF_8);
        } catch (final UnsupportedEncodingException e) {
            throw new AccumuloElementConversionException(e.getMessage(), e);
        }
        try {
            final Edge edge = new Edge(group, ((ToBytesSerialiser) schema.getVertexSerialiser()).deserialise(result[0]),
                    ((ToBytesSerialiser) schema.getVertexSerialiser()).deserialise(result[1]), direction.isDirected(), matchedVertex, null);
            addPropertiesToElement(edge, key);
            return edge;
        } catch (final SerialisationException e) {
            throw new AccumuloElementConversionException("Failed to re-create Edge from key", e);
        }
    }

    protected byte[] getSerialisedSource(final Edge edge) {
        try {
            return ByteArrayEscapeUtils.escape(((ToBytesSerialiser) schema.getVertexSerialiser()).serialise(edge.getSource()));
        } catch (final SerialisationException e) {
            throw new AccumuloElementConversionException("Failed to serialise Edge Source", e);
        }
    }

    protected byte[] getSerialisedDestination(final Edge edge) {
        try {
            return ByteArrayEscapeUtils.escape(((ToBytesSerialiser) schema.getVertexSerialiser()).serialise(edge.getDestination()));
        } catch (final SerialisationException e) {
            throw new AccumuloElementConversionException("Failed to serialise Edge Destination", e);
        }
    }

    protected String getGroupFromKey(final Key key) {
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

    private Object getDeserialisedObject(final ToBytesSerialiser serialiser, final byte[] bytes, final int from, final int length) throws SerialisationException {
        //Don't initialise with  #deserialiseEmpty() as this might initialise an complex empty structure to be immediately overwritten e.g. TreeSet<String>
        Object deserialisedObject;
        if (length > 0) {
            deserialisedObject = serialiser.deserialise(bytes, from, length);
        } else {
            deserialisedObject = serialiser.deserialiseEmpty();
        }
        return deserialisedObject;
    }

    private boolean isNotEmpty(final Value value) {
        return null != value && value.getSize() != 0;
    }

    private int getCurrentPropLength(final byte[] bytes, final int pos) {
        try {
            //This value will be no bigger than an int, no casting issues should occur.
            return (int) CompactRawSerialisationUtils.readLong(bytes, pos);
        } catch (final SerialisationException e) {
            throw new AccumuloElementConversionException("Exception reading length of property", e);
        }
    }

}
