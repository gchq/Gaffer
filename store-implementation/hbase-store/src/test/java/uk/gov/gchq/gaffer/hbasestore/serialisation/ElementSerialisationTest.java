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

import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.binaryoperator.FreqMapAggregator;
import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.hbasestore.util.HBasePropertyNames;
import uk.gov.gchq.gaffer.hbasestore.utils.HBaseStoreConstants;
import uk.gov.gchq.gaffer.serialisation.FreqMapSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.types.FreqMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Copied and adapted from the AcummuloStore ElementConverterTests
 */
public class ElementSerialisationTest {
    private ElementSerialisation serialisation;

    @Before
    public void setUp() throws SchemaException, IOException {
        final Schema schema = Schema.fromJson(StreamUtil.schemas(getClass()));
        serialisation = new ElementSerialisation(schema);
    }

    //TEST WE CAN RETRIEVE AN ELEMENT FROM A KEY THAT HAS BEEN CREATED CORRECTLY
    @Test
    public void shouldReturnHBaseKeySerialisationFromBasicEdge() throws SchemaException, IOException {
        // Given
        final Edge edge = new Edge(TestGroups.EDGE);
        edge.setDestination("2");
        edge.setSource("1");
        edge.setDirected(true);

        // When
        final Pair<byte[], byte[]> keys = serialisation.getRowKeys(edge);

        // Then
        final Edge newEdge = (Edge) serialisation.getPartialElement(TestGroups.EDGE, keys.getFirst());
        assertEquals("1", newEdge.getSource());
        assertEquals("2", newEdge.getDestination());
        assertEquals(true, newEdge.isDirected());
    }

    @Test
    public void shouldReturnHBaseKeySerialisationFromBasicEntity() throws SchemaException, IOException {
        // Given
        final Entity entity = new Entity(TestGroups.ENTITY);
        entity.setVertex("3");

        // When
        final byte[] key = serialisation.getRowKey(entity);

        // Then
        final Entity newEntity = (Entity) serialisation.getPartialElement(TestGroups.ENTITY, key);
        assertEquals("3", newEntity.getVertex());
    }

    @Test
    public void shouldReturnHBaseKeySerialisationFromCFCQPropertyEdge() throws SchemaException, IOException {
        // Given
        final Edge edge = new Edge(TestGroups.EDGE);
        edge.putProperty(HBasePropertyNames.COLUMN_QUALIFIER, 100);

        // When
        final byte[] columnQualifier = serialisation.getColumnQualifier(edge);
        final Properties properties = serialisation.getPropertiesFromColumnQualifier(TestGroups.EDGE, columnQualifier);

        // Then
        assertEquals(100, properties.get(HBasePropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldReturnHBaseKeySerialisationFromCFCQPropertyEntity() throws SchemaException, IOException {
        // Given
        final Entity entity = new Entity(TestGroups.ENTITY);
        entity.putProperty(HBasePropertyNames.COLUMN_QUALIFIER, 100);

        // When
        final byte[] columnQualifier = serialisation.getColumnQualifier(entity);
        final Properties properties = serialisation.getPropertiesFromColumnQualifier(TestGroups.ENTITY, columnQualifier);

        // Then
        assertEquals(100, properties.get(HBasePropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldReturnHBaseKeySerialisationMultipleCQPropertyEdge() throws SchemaException, IOException {
        // Given
        final Edge edge = new Edge(TestGroups.EDGE);
        edge.setDestination("2");
        edge.setSource("1");
        edge.setDirected(true);
        edge.putProperty(HBasePropertyNames.COLUMN_QUALIFIER, 100);

        // When
        final byte[] columnQualifier = serialisation.getColumnQualifier(edge);
        final Properties properties = serialisation.getPropertiesFromColumnQualifier(TestGroups.EDGE, columnQualifier);

        // Then
        assertEquals(100, properties.get(HBasePropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldReturnHBaseKeySerialisationMultipleCQPropertiesEntity() throws SchemaException, IOException {
        // Given
        final Entity entity = new Entity(TestGroups.ENTITY);
        entity.setVertex("3");
        entity.putProperty(HBasePropertyNames.COLUMN_QUALIFIER, 100);

        // When
        final byte[] columnQualifier = serialisation.getColumnQualifier(entity);
        final Properties properties = serialisation.getPropertiesFromColumnQualifier(TestGroups.ENTITY, columnQualifier);

        // Then
        assertEquals(100, properties.get(HBasePropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldGetOriginalEdgeWithMatchAsSourceNotSet() throws SchemaException, IOException {
        // Given
        final Edge edge = new Edge(TestGroups.EDGE);
        edge.setDestination("2");
        edge.setSource("1");
        edge.setDirected(true);

        final Pair<byte[], byte[]> keys = serialisation.getRowKeys(edge);
        final Map<String, String> options = new HashMap<>();

        // When
        final Edge newEdge = (Edge) serialisation.getPartialElement(TestGroups.EDGE, keys.getSecond(), options);

        // Then
        assertEquals("1", newEdge.getSource());
        assertEquals("2", newEdge.getDestination());
        assertEquals(true, newEdge.isDirected());
    }

    @Test
    public void shouldGetFlippedEdgeWithMatchAsSourceFalse() throws SchemaException, IOException {
        // Given
        final Edge edge = new Edge(TestGroups.EDGE);
        edge.setDestination("2");
        edge.setSource("1");
        edge.setDirected(true);

        final Pair<byte[], byte[]> keys = serialisation.getRowKeys(edge);
        final Map<String, String> options = new HashMap<>();
        options.put(HBaseStoreConstants.OPERATION_RETURN_MATCHED_SEEDS_AS_EDGE_SOURCE, "true");

        // When
        final Edge newEdge = (Edge) serialisation.getPartialElement(TestGroups.EDGE, keys.getSecond(), options);

        // Then
        assertEquals("2", newEdge.getSource());
        assertEquals("1", newEdge.getDestination());
        assertEquals(true, newEdge.isDirected());
    }

    @Test
    public void shouldSkipNullPropertyValuesWhenCreatingHBaseKey() throws SchemaException, IOException {
        // Given
        final Edge edge = new Edge(TestGroups.EDGE);
        edge.setSource("1");
        edge.setDestination("2");
        edge.setDirected(true);
        edge.putProperty(HBasePropertyNames.COLUMN_QUALIFIER, null);

        // When
        final byte[] columnQualifier = serialisation.getColumnQualifier(edge);
        Properties properties = serialisation.getPropertiesFromColumnQualifier(TestGroups.EDGE, columnQualifier);

        // Then
        assertEquals(null, properties.get(HBasePropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldSerialiseAndDeSerialiseBetweenPropertyAndValue() throws Exception {
        Properties properties = new Properties();
        properties.put(HBasePropertyNames.PROP_1, 60);
        properties.put(HBasePropertyNames.PROP_2, 166);
        properties.put(HBasePropertyNames.PROP_3, 299);
        properties.put(HBasePropertyNames.PROP_4, 10);
        properties.put(HBasePropertyNames.COUNT, 8);

        final byte[] value = serialisation.getValue(TestGroups.EDGE, properties);
        final Properties deSerialisedProperties = serialisation.getPropertiesFromValue(TestGroups.EDGE, value);
        assertEquals(60, deSerialisedProperties.get(HBasePropertyNames.PROP_1));
        assertEquals(166, deSerialisedProperties.get(HBasePropertyNames.PROP_2));
        assertEquals(299, deSerialisedProperties.get(HBasePropertyNames.PROP_3));
        assertEquals(10, deSerialisedProperties.get(HBasePropertyNames.PROP_4));
        assertEquals(8, deSerialisedProperties.get(HBasePropertyNames.COUNT));
    }

    @Test
    public void shouldSerialiseAndDeSerialiseBetweenPropertyAndValueMissingMiddleProperty() throws Exception {
        Properties properties = new Properties();
        properties.put(HBasePropertyNames.PROP_1, 60);
        properties.put(HBasePropertyNames.PROP_3, 299);
        properties.put(HBasePropertyNames.PROP_4, 10);
        properties.put(HBasePropertyNames.COUNT, 8);

        final byte[] value = serialisation.getValue(TestGroups.EDGE, properties);
        final Properties deSerialisedProperties = serialisation.getPropertiesFromValue(TestGroups.EDGE, value);
        assertEquals(60, deSerialisedProperties.get(HBasePropertyNames.PROP_1));
        assertEquals(299, deSerialisedProperties.get(HBasePropertyNames.PROP_3));
        assertEquals(10, deSerialisedProperties.get(HBasePropertyNames.PROP_4));
        assertEquals(8, deSerialisedProperties.get(HBasePropertyNames.COUNT));
    }

    @Test
    public void shouldSerialiseAndDeSerialiseBetweenPropertyAndValueMissingEndProperty() throws Exception {
        Properties properties = new Properties();
        properties.put(HBasePropertyNames.PROP_1, 60);
        properties.put(HBasePropertyNames.PROP_2, 166);
        properties.put(HBasePropertyNames.PROP_3, 299);
        properties.put(HBasePropertyNames.PROP_4, 10);

        final byte[] value = serialisation.getValue(TestGroups.EDGE, properties);
        final Properties deSerialisedProperties = serialisation.getPropertiesFromValue(TestGroups.EDGE, value);
        assertEquals(60, deSerialisedProperties.get(HBasePropertyNames.PROP_1));
        assertEquals(166, deSerialisedProperties.get(HBasePropertyNames.PROP_2));
        assertEquals(299, deSerialisedProperties.get(HBasePropertyNames.PROP_3));
        assertEquals(10, deSerialisedProperties.get(HBasePropertyNames.PROP_4));
    }

    @Test
    public void shouldSerialiseAndDeSerialiseBetweenPropertyAndValueMissingStartProperty() throws Exception {
        Properties properties = new Properties();
        properties.put(HBasePropertyNames.PROP_2, 166);
        properties.put(HBasePropertyNames.PROP_3, 299);
        properties.put(HBasePropertyNames.PROP_4, 10);
        properties.put(HBasePropertyNames.COUNT, 8);

        final byte[] value = serialisation.getValue(TestGroups.EDGE, properties);
        final Properties deSerialisedProperties = serialisation.getPropertiesFromValue(TestGroups.EDGE, value);
        assertEquals(166, deSerialisedProperties.get(HBasePropertyNames.PROP_2));
        assertEquals(299, deSerialisedProperties.get(HBasePropertyNames.PROP_3));
        assertEquals(10, deSerialisedProperties.get(HBasePropertyNames.PROP_4));
        assertEquals(8, deSerialisedProperties.get(HBasePropertyNames.COUNT));
    }

    @Test
    public void shouldSerialiseAndDeSerialiseBetweenPropertyAndValueWithNullProperty() throws Exception {
        Properties properties = new Properties();
        properties.put(HBasePropertyNames.PROP_1, 5);
        properties.put(HBasePropertyNames.PROP_2, null);
        properties.put(HBasePropertyNames.PROP_3, 299);
        properties.put(HBasePropertyNames.PROP_4, 10);
        properties.put(HBasePropertyNames.COUNT, 8);

        final byte[] value = serialisation.getValue(TestGroups.EDGE, properties);
        final Properties deSerialisedProperties = serialisation.getPropertiesFromValue(TestGroups.EDGE, value);
        assertEquals(5, deSerialisedProperties.get(HBasePropertyNames.PROP_1));
        assertNull(deSerialisedProperties.get(HBasePropertyNames.PROP_2));
        assertEquals(299, deSerialisedProperties.get(HBasePropertyNames.PROP_3));
        assertEquals(10, deSerialisedProperties.get(HBasePropertyNames.PROP_4));
        assertEquals(8, deSerialisedProperties.get(HBasePropertyNames.COUNT));
    }

    @Test
    public void shouldTruncatePropertyBytes() throws Exception {
        // Given
        final Properties properties = new Properties() {
            {
                put(HBasePropertyNames.COLUMN_QUALIFIER, 1);
                put(HBasePropertyNames.COLUMN_QUALIFIER_2, 2);
                put(HBasePropertyNames.COLUMN_QUALIFIER_3, 3);
                put(HBasePropertyNames.COLUMN_QUALIFIER_4, 4);
            }
        };

        final byte[] bytes = serialisation.getColumnQualifier(TestGroups.EDGE, properties);

        // When
        final byte[] truncatedPropertyBytes = serialisation.getPropertiesAsBytesFromColumnQualifier(TestGroups.EDGE, bytes, 2);

        // Then
        final Properties truncatedProperties = new Properties() {
            {
                put(HBasePropertyNames.COLUMN_QUALIFIER, 1);
                put(HBasePropertyNames.COLUMN_QUALIFIER_2, 2);
            }
        };
        final byte[] expectedBytes = serialisation.getColumnQualifier(TestGroups.EDGE, truncatedProperties);
        final byte[] expectedTruncatedPropertyBytes = serialisation.getPropertiesAsBytesFromColumnQualifier(TestGroups.EDGE, expectedBytes, 2);
        assertArrayEquals(expectedTruncatedPropertyBytes, truncatedPropertyBytes);
    }

    @Test
    public void shouldTruncatePropertyBytesWithEmptyBytes() throws Exception {
        // Given
        final byte[] bytes = HBaseStoreConstants.EMPTY_BYTES;

        // When
        final byte[] truncatedBytes = serialisation.getPropertiesAsBytesFromColumnQualifier(TestGroups.EDGE, bytes, 2);

        // Then
        assertEquals(0, truncatedBytes.length);
    }

    @Test
    public void shouldBuildTimestampFromProperty() throws Exception {
        // Given
        // add extra timestamp property to schema
        final Schema schema = new Schema.Builder()
                .json(StreamUtil.schemas(getClass()))
                .build();
        serialisation = new ElementSerialisation(new Schema.Builder(schema)
                .type("timestamp", Long.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(HBasePropertyNames.TIMESTAMP, "timestamp")
                        .build())
                .timestampProperty(HBasePropertyNames.TIMESTAMP)
                .build());

        final long propertyTimestamp = 10L;
        final Properties properties = new Properties() {
            {
                put(HBasePropertyNames.COLUMN_QUALIFIER, 1);
                put(HBasePropertyNames.PROP_1, 2);
                put(HBasePropertyNames.TIMESTAMP, propertyTimestamp);
            }
        };

        // When
        final long timestamp = serialisation.getTimestamp(properties);

        // Then
        assertEquals(propertyTimestamp, timestamp);
    }

    @Test
    public void shouldBuildTimestampFromDefaultTimeWhenPropertyIsNull() throws Exception {
        // Given
        // add extra timestamp property to schema
        final Schema schema = new Schema.Builder().json(StreamUtil.schemas(getClass())).build();
        serialisation = new ElementSerialisation(new Schema.Builder(schema)
                .type("timestamp", Long.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(HBasePropertyNames.TIMESTAMP, "timestamp")
                        .build())
                .timestampProperty(HBasePropertyNames.TIMESTAMP)
                .build());

        final Long propertyTimestamp = null;
        final Properties properties = new Properties() {
            {
                put(HBasePropertyNames.COLUMN_QUALIFIER, 1);
                put(HBasePropertyNames.PROP_1, 2);
                put(HBasePropertyNames.TIMESTAMP, propertyTimestamp);
            }
        };

        // When
        final long timestamp = serialisation.getTimestamp(properties);

        // Then
        assertNotNull(timestamp);
    }

    @Test
    public void shouldBuildTimestampFromDefaultTime() throws Exception {
        // Given
        final Properties properties = new Properties() {
            {
                put(HBasePropertyNames.COLUMN_QUALIFIER, 1);
                put(HBasePropertyNames.PROP_1, 2);
            }
        };

        // When
        final long timestamp = serialisation.getTimestamp(properties);

        // Then
        assertNotNull(timestamp);
    }

    @Test
    public void shouldGetPropertiesFromTimestamp() throws Exception {
        // Given
        // add extra timestamp property to schema
        final Schema schema = new Schema.Builder().json(StreamUtil.schemas(getClass())).build();
        serialisation = new ElementSerialisation(new Schema.Builder(schema)
                .type("timestamp", Long.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(HBasePropertyNames.TIMESTAMP, "timestamp")
                        .build())
                .timestampProperty(HBasePropertyNames.TIMESTAMP)
                .build());

        final long timestamp = System.currentTimeMillis();
        final String group = TestGroups.EDGE;

        // When
        final Properties properties = serialisation.getPropertiesFromTimestamp(group, timestamp);

        // Then
        assertEquals(1, properties.size());
        assertEquals(timestamp, properties.get(HBasePropertyNames.TIMESTAMP));
    }

    @Test
    public void shouldGetEmptyPropertiesFromTimestampWhenNoTimestampPropertyInGroup() throws Exception {
        // Given
        // add timestamp property name but don't add the property to the edge group
        final Schema schema = new Schema.Builder().json(StreamUtil.schemas(getClass())).build();
        serialisation = new ElementSerialisation(new Schema.Builder(schema)
                .timestampProperty(HBasePropertyNames.TIMESTAMP)
                .build());

        final long timestamp = System.currentTimeMillis();
        final String group = TestGroups.EDGE;

        // When
        final Properties properties = serialisation.getPropertiesFromTimestamp(group, timestamp);

        // Then
        assertEquals(0, properties.size());
    }

    @Test
    public void shouldGetEmptyPropertiesFromTimestampWhenNoTimestampProperty() throws Exception {
        // Given
        final long timestamp = System.currentTimeMillis();
        final String group = TestGroups.EDGE;

        // When
        final Properties properties = serialisation.getPropertiesFromTimestamp(group, timestamp);

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
            serialisation.getPropertiesFromTimestamp(group, timestamp);
            fail("Exception expected");
        } catch (final Exception e) {
            assertNotNull(e.getMessage());
        }
    }


    @Test
    public void shouldSerialiseAndDeserialisePropertiesWhenAllAreEmpty()
            throws Exception {
        // Given 
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                                .vertex("string")
                                .property(HBasePropertyNames.PROP_1, "map")
                                .property(HBasePropertyNames.PROP_2, "map")
                                .build()
                )
                .type("string", String.class)
                .type("map", new TypeDefinition.Builder()
                        .clazz(FreqMap.class)
                        .aggregateFunction(new FreqMapAggregator())
                        .serialiser(new FreqMapSerialiser())
                        .build())
                .build();

        serialisation = new ElementSerialisation(schema);

        final Entity entity = new Entity.Builder()
                .vertex("vertex1")
                .property(HBasePropertyNames.PROP_1, new FreqMap())
                .property(HBasePropertyNames.PROP_2, new FreqMap())
                .build();

        // When 1 
        final byte[] value = serialisation.getValue(TestGroups.ENTITY, entity.getProperties());

        // Then 1
        assertArrayEquals(new byte[]{ByteArrayEscapeUtils.DELIMITER, ByteArrayEscapeUtils.DELIMITER}, value);

        // When 2
        final Properties properties = serialisation.getPropertiesFromValue(
                TestGroups.ENTITY, value);

        // Then 2
        assertEquals(entity.getProperties(), properties);
    }
}