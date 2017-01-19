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
package uk.gov.gchq.gaffer.accumulostore.key.core.impl;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloPropertyNames;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.function.aggregate.FreqMapAggregator;
import uk.gov.gchq.gaffer.serialisation.FreqMapSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.types.FreqMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractAccumuloElementConverterTest {

    private AccumuloElementConverter converter;

    @Before
    public void setUp() throws SchemaException, IOException {
        final Schema schema = Schema.fromJson(StreamUtil.schemas(getClass()));
        converter = createConverter(schema);
    }

    protected abstract AccumuloElementConverter createConverter(final Schema schema);

    //TEST WE CAN RETRIEVE AN ELEMENT FROM A KEY THAT HAS BEEN CREATED CORRECTLY
    @Test
    public void shouldReturnAccumuloKeyConverterFromBasicEdge() throws SchemaException, AccumuloElementConversionException, IOException {
        // Given
        final Edge edge = new Edge(TestGroups.EDGE);
        edge.setDestination("2");
        edge.setSource("1");
        edge.setDirected(true);

        // When
        final Pair<Key> keys = converter.getKeysFromElement(edge);

        // Then
        final Edge newEdge = (Edge) converter.getElementFromKey(keys.getFirst());
        assertEquals("1", newEdge.getSource());
        assertEquals("2", newEdge.getDestination());
        assertEquals(true, newEdge.isDirected());
    }

    @Test
    public void shouldReturnAccumuloKeyConverterFromBasicEntity() throws SchemaException, AccumuloElementConversionException, IOException {
        // Given
        final Entity entity = new Entity(TestGroups.ENTITY);
        entity.setVertex("3");

        // When
        final Key key = converter.getKeyFromEntity(entity);

        // Then
        final Entity newEntity = (Entity) converter.getElementFromKey(key);
        assertEquals("3", newEntity.getVertex());
    }

    @Test
    public void shouldReturnAccumuloKeyConverterFromCFCQPropertydEdge() throws SchemaException, AccumuloElementConversionException, IOException {
        // Given
        final Edge edge = new Edge(TestGroups.EDGE);
        edge.setDestination("2");
        edge.setSource("1");
        edge.setDirected(false);
        edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 100);

        // When
        final Pair<Key> keys = converter.getKeysFromElement(edge);
        final Edge newEdge = (Edge) converter.getElementFromKey(keys.getFirst());

        // Then
        assertEquals("1", newEdge.getSource());
        assertEquals("2", newEdge.getDestination());
        assertEquals(false, newEdge.isDirected());
        assertEquals(100, newEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldReturnAccumuloKeyConverterFromCFCQPropertydEntity() throws SchemaException, AccumuloElementConversionException, IOException {
        // Given
        final Entity entity = new Entity(TestGroups.ENTITY);
        entity.setVertex("3");
        entity.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 100);

        // When
        final Pair<Key> keys = converter.getKeysFromElement(entity);
        final Entity newEntity = (Entity) converter.getElementFromKey(keys.getFirst());

        // Then
        assertEquals("3", newEntity.getVertex());
        assertEquals(100, newEntity.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldReturnAccumuloKeyConverterMultipleCQPropertydEdge() throws SchemaException, AccumuloElementConversionException, IOException {
        // Given
        final Edge edge = new Edge(TestGroups.EDGE);
        edge.setDestination("2");
        edge.setSource("1");
        edge.setDirected(true);
        edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 100);

        // When
        final Pair<Key> keys = converter.getKeysFromElement(edge);
        final Edge newEdge = (Edge) converter.getElementFromKey(keys.getSecond());

        // Then
        assertEquals("1", newEdge.getSource());
        assertEquals("2", newEdge.getDestination());
        assertEquals(true, newEdge.isDirected());
        assertEquals(100, newEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldReturnAccumuloKeyConverterMultipleCQPropertiesEntity() throws SchemaException, AccumuloElementConversionException, IOException {
        // Given
        final Entity entity = new Entity(TestGroups.ENTITY);
        entity.setVertex("3");
        entity.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 100);

        // When
        final Pair<Key> keys = converter.getKeysFromElement(entity);
        final Entity newEntity = (Entity) converter.getElementFromKey(keys.getFirst());

        // Then
        assertEquals("3", newEntity.getVertex());
        assertEquals(100, newEntity.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldGetOriginalEdgeWithMatchAsSourceNotSet() throws SchemaException, AccumuloElementConversionException, IOException {
        // Given
        final Edge edge = new Edge(TestGroups.EDGE);
        edge.setDestination("2");
        edge.setSource("1");
        edge.setDirected(true);
        edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 100);

        final Pair<Key> keys = converter.getKeysFromElement(edge);
        final Map<String, String> options = new HashMap<>();

        // When
        final Edge newEdge = (Edge) converter.getElementFromKey(keys.getSecond(), options);

        // Then
        assertEquals("1", newEdge.getSource());
        assertEquals("2", newEdge.getDestination());
        assertEquals(true, newEdge.isDirected());
        assertEquals(100, newEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldGetFlippedEdgeWithMatchAsSourceFalse() throws SchemaException, AccumuloElementConversionException, IOException {
        // Given
        final Edge edge = new Edge(TestGroups.EDGE);
        edge.setDestination("2");
        edge.setSource("1");
        edge.setDirected(true);
        edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 100);

        final Pair<Key> keys = converter.getKeysFromElement(edge);
        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.OPERATION_RETURN_MATCHED_SEEDS_AS_EDGE_SOURCE, "true");

        // When
        final Edge newEdge = (Edge) converter.getElementFromKey(keys.getSecond(), options);

        // Then
        assertEquals("2", newEdge.getSource());
        assertEquals("1", newEdge.getDestination());
        assertEquals(true, newEdge.isDirected());
        assertEquals(100, newEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldSkipNullPropertyValuesWhenCreatingAccumuloKey() throws SchemaException, AccumuloElementConversionException, IOException {
        // Given
        final Edge edge = new Edge(TestGroups.EDGE);
        edge.setSource("1");
        edge.setDestination("2");
        edge.setDirected(true);
        edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, null);

        // When
        final Pair<Key> keys = converter.getKeysFromElement(edge);
        Properties properties = converter.getPropertiesFromColumnQualifier(TestGroups.EDGE, keys.getFirst().getColumnQualifierData().getBackingArray());

        // Then
        assertEquals(null, properties.get(AccumuloPropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldSerialiseAndDeSerialiseBetweenPropertyAndValue() throws AccumuloElementConversionException {
        Properties properties = new Properties();
        properties.put(AccumuloPropertyNames.PROP_1, 60);
        properties.put(AccumuloPropertyNames.PROP_2, 166);
        properties.put(AccumuloPropertyNames.PROP_3, 299);
        properties.put(AccumuloPropertyNames.PROP_4, 10);
        properties.put(AccumuloPropertyNames.COUNT, 8);

        final Value value = converter.getValueFromProperties(TestGroups.EDGE, properties);
        final Properties deSerialisedProperties = converter.getPropertiesFromValue(TestGroups.EDGE, value);
        assertEquals(60, deSerialisedProperties.get(AccumuloPropertyNames.PROP_1));
        assertEquals(166, deSerialisedProperties.get(AccumuloPropertyNames.PROP_2));
        assertEquals(299, deSerialisedProperties.get(AccumuloPropertyNames.PROP_3));
        assertEquals(10, deSerialisedProperties.get(AccumuloPropertyNames.PROP_4));
        assertEquals(8, deSerialisedProperties.get(AccumuloPropertyNames.COUNT));
    }

    @Test
    public void shouldSerialiseAndDeSerialiseBetweenPropertyAndValueMissingMiddleProperty() throws AccumuloElementConversionException {
        Properties properties = new Properties();
        properties.put(AccumuloPropertyNames.PROP_1, 60);
        properties.put(AccumuloPropertyNames.PROP_3, 299);
        properties.put(AccumuloPropertyNames.PROP_4, 10);
        properties.put(AccumuloPropertyNames.COUNT, 8);

        final Value value = converter.getValueFromProperties(TestGroups.EDGE, properties);
        final Properties deSerialisedProperties = converter.getPropertiesFromValue(TestGroups.EDGE, value);
        assertEquals(60, deSerialisedProperties.get(AccumuloPropertyNames.PROP_1));
        assertEquals(299, deSerialisedProperties.get(AccumuloPropertyNames.PROP_3));
        assertEquals(10, deSerialisedProperties.get(AccumuloPropertyNames.PROP_4));
        assertEquals(8, deSerialisedProperties.get(AccumuloPropertyNames.COUNT));
    }

    @Test
    public void shouldSerialiseAndDeSerialiseBetweenPropertyAndValueMissingEndProperty() throws AccumuloElementConversionException {
        Properties properties = new Properties();
        properties.put(AccumuloPropertyNames.PROP_1, 60);
        properties.put(AccumuloPropertyNames.PROP_2, 166);
        properties.put(AccumuloPropertyNames.PROP_3, 299);
        properties.put(AccumuloPropertyNames.PROP_4, 10);

        final Value value = converter.getValueFromProperties(TestGroups.EDGE, properties);
        final Properties deSerialisedProperties = converter.getPropertiesFromValue(TestGroups.EDGE, value);
        assertEquals(60, deSerialisedProperties.get(AccumuloPropertyNames.PROP_1));
        assertEquals(166, deSerialisedProperties.get(AccumuloPropertyNames.PROP_2));
        assertEquals(299, deSerialisedProperties.get(AccumuloPropertyNames.PROP_3));
        assertEquals(10, deSerialisedProperties.get(AccumuloPropertyNames.PROP_4));
    }

    @Test
    public void shouldSerialiseAndDeSerialiseBetweenPropertyAndValueMissingStartProperty() throws AccumuloElementConversionException {
        Properties properties = new Properties();
        properties.put(AccumuloPropertyNames.PROP_2, 166);
        properties.put(AccumuloPropertyNames.PROP_3, 299);
        properties.put(AccumuloPropertyNames.PROP_4, 10);
        properties.put(AccumuloPropertyNames.COUNT, 8);

        final Value value = converter.getValueFromProperties(TestGroups.EDGE, properties);
        final Properties deSerialisedProperties = converter.getPropertiesFromValue(TestGroups.EDGE, value);
        assertEquals(166, deSerialisedProperties.get(AccumuloPropertyNames.PROP_2));
        assertEquals(299, deSerialisedProperties.get(AccumuloPropertyNames.PROP_3));
        assertEquals(10, deSerialisedProperties.get(AccumuloPropertyNames.PROP_4));
        assertEquals(8, deSerialisedProperties.get(AccumuloPropertyNames.COUNT));
    }

    @Test
    public void shouldSerialiseAndDeSerialiseBetweenPropertyAndValueWithNullProperty() throws AccumuloElementConversionException {
        Properties properties = new Properties();
        properties.put(AccumuloPropertyNames.PROP_1, 5);
        properties.put(AccumuloPropertyNames.PROP_2, null);
        properties.put(AccumuloPropertyNames.PROP_3, 299);
        properties.put(AccumuloPropertyNames.PROP_4, 10);
        properties.put(AccumuloPropertyNames.COUNT, 8);

        final Value value = converter.getValueFromProperties(TestGroups.EDGE, properties);
        final Properties deSerialisedProperties = converter.getPropertiesFromValue(TestGroups.EDGE, value);
        assertEquals(5, deSerialisedProperties.get(AccumuloPropertyNames.PROP_1));
        assertNull(deSerialisedProperties.get(AccumuloPropertyNames.PROP_2));
        assertEquals(299, deSerialisedProperties.get(AccumuloPropertyNames.PROP_3));
        assertEquals(10, deSerialisedProperties.get(AccumuloPropertyNames.PROP_4));
        assertEquals(8, deSerialisedProperties.get(AccumuloPropertyNames.COUNT));
    }

    @Test
    public void shouldTruncatePropertyBytes() throws AccumuloElementConversionException {
        // Given
        final Properties properties = new Properties() {
            {
                put(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
                put(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 2);
                put(AccumuloPropertyNames.COLUMN_QUALIFIER_3, 3);
                put(AccumuloPropertyNames.COLUMN_QUALIFIER_4, 4);
            }
        };

        final byte[] bytes = converter.buildColumnQualifier(TestGroups.EDGE, properties);

        // When
        final byte[] truncatedBytes = converter.getPropertiesAsBytesFromColumnQualifier(TestGroups.EDGE, bytes, 2);

        // Then
        final Properties truncatedProperties = new Properties() {
            {
                put(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
                put(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 2);
            }
        };
        assertEquals(truncatedProperties, converter.getPropertiesFromColumnQualifier(TestGroups.EDGE, truncatedBytes));
    }

    @Test
    public void shouldTruncatePropertyBytesWithEmptyBytes() throws AccumuloElementConversionException {
        // Given
        final byte[] bytes = AccumuloStoreConstants.EMPTY_BYTES;

        // When
        final byte[] truncatedBytes = converter.getPropertiesAsBytesFromColumnQualifier(TestGroups.EDGE, bytes, 2);

        // Then
        assertEquals(0, truncatedBytes.length);
    }

    @Test
    public void shouldBuildTimestampFromProperty() throws AccumuloElementConversionException {
        // Given
        // add extra timestamp property to schema
        final Schema schema = new Schema.Builder()
                .json(StreamUtil.schemas(getClass()))
                .build();
        converter = createConverter(new Schema.Builder(schema)
                .type("timestamp", Long.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(AccumuloPropertyNames.TIMESTAMP, "timestamp")
                        .build())
                .timestampProperty(AccumuloPropertyNames.TIMESTAMP)
                .build());

        final long propertyTimestamp = 10L;
        final Properties properties = new Properties() {
            {
                put(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
                put(AccumuloPropertyNames.PROP_1, 2);
                put(AccumuloPropertyNames.TIMESTAMP, propertyTimestamp);
            }
        };

        // When
        final long timestamp = converter.buildTimestamp(properties);

        // Then
        assertEquals(propertyTimestamp, timestamp);
    }

    @Test
    public void shouldBuildTimestampFromDefaultTimeWhenPropertyIsNull() throws AccumuloElementConversionException {
        // Given
        // add extra timestamp property to schema
        final Schema schema = new Schema.Builder().json(StreamUtil.schemas(getClass())).build();
        converter = createConverter(new Schema.Builder(schema)
                .type("timestamp", Long.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(AccumuloPropertyNames.TIMESTAMP, "timestamp")
                        .build())
                .timestampProperty(AccumuloPropertyNames.TIMESTAMP)
                .build());

        final Long propertyTimestamp = null;
        final Properties properties = new Properties() {
            {
                put(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
                put(AccumuloPropertyNames.PROP_1, 2);
                put(AccumuloPropertyNames.TIMESTAMP, propertyTimestamp);
            }
        };

        // When
        final long timestamp = converter.buildTimestamp(properties);

        // Then
        assertNotNull(timestamp);
    }

    @Test
    public void shouldBuildTimestampFromDefaultTime() throws AccumuloElementConversionException {
        // Given
        final Properties properties = new Properties() {
            {
                put(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
                put(AccumuloPropertyNames.PROP_1, 2);
            }
        };

        // When
        final long timestamp = converter.buildTimestamp(properties);

        // Then
        assertNotNull(timestamp);
    }

    @Test
    public void shouldGetPropertiesFromTimestamp() throws AccumuloElementConversionException {
        // Given
        // add extra timestamp property to schema
        final Schema schema = new Schema.Builder().json(StreamUtil.schemas(getClass())).build();
        converter = createConverter(new Schema.Builder(schema)
                .type("timestamp", Long.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(AccumuloPropertyNames.TIMESTAMP, "timestamp")
                        .build())
                .timestampProperty(AccumuloPropertyNames.TIMESTAMP)
                .build());

        final long timestamp = System.currentTimeMillis();
        final String group = TestGroups.EDGE;

        // When
        final Properties properties = converter.getPropertiesFromTimestamp(group, timestamp);

        // Then
        assertEquals(1, properties.size());
        assertEquals(timestamp, properties.get(AccumuloPropertyNames.TIMESTAMP));
    }

    @Test
    public void shouldGetEmptyPropertiesFromTimestampWhenNoTimestampPropertyInGroup() throws AccumuloElementConversionException {
        // Given
        // add timestamp property name but don't add the property to the edge group
        final Schema schema = new Schema.Builder().json(StreamUtil.schemas(getClass())).build();
        converter = createConverter(new Schema.Builder(schema)
                .timestampProperty(AccumuloPropertyNames.TIMESTAMP)
                .build());

        final long timestamp = System.currentTimeMillis();
        final String group = TestGroups.EDGE;

        // When
        final Properties properties = converter.getPropertiesFromTimestamp(group, timestamp);

        // Then
        assertEquals(0, properties.size());
    }

    @Test
    public void shouldGetEmptyPropertiesFromTimestampWhenNoTimestampProperty() throws AccumuloElementConversionException {
        // Given
        final long timestamp = System.currentTimeMillis();
        final String group = TestGroups.EDGE;

        // When
        final Properties properties = converter.getPropertiesFromTimestamp(group, timestamp);

        // Then
        assertEquals(0, properties.size());
    }

    @Test
    public void shouldThrowExceptionWhenGetPropertiesFromTimestampWhenGroupIsNotFound() {
        // Given
        final long timestamp = System.currentTimeMillis();
        final String group = "unknownGroup";

        // When / Then
        try {
            converter.getPropertiesFromTimestamp(group, timestamp);
            fail("Exception expected");
        } catch (final AccumuloElementConversionException e) {
            assertNotNull(e.getMessage());
        }
    }


    @Test
    public void shouldSerialiseAndDeserialisePropertiesWhenAllAreEmpty()
            throws AccumuloElementConversionException {
        // Given 
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                                .vertex("string")
                                .property(TestPropertyNames.PROP_1, "map")
                                .property(TestPropertyNames.PROP_2, "map")
                                .build()
                )
                .type("string", String.class)
                .type("map", new TypeDefinition.Builder()
                        .clazz(FreqMap.class)
                        .aggregateFunction(new FreqMapAggregator())
                        .serialiser(new FreqMapSerialiser())
                        .build())
                .build();

        converter = createConverter(schema);

        final Entity entity = new Entity.Builder()
                .vertex("vertex1")
                .property(TestPropertyNames.PROP_1, new FreqMap())
                .property(TestPropertyNames.PROP_2, new FreqMap())
                .build();

        // When 1 
        final Value value = converter.getValueFromProperties(TestGroups.ENTITY, entity.getProperties());

        // Then 1
        assertTrue(value.getSize() > 0);

        // When 2
        final Properties properties = converter.getPropertiesFromValue(
                TestGroups.ENTITY, value);

        // Then 2
        assertEquals(entity.getProperties(), properties);
    }
}