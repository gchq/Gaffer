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
package uk.gov.gchq.gaffer.accumulostore.key;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloPropertyNames;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.accumulostore.utils.BytesAndRange;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.serialisation.FreqMapSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.gaffer.types.function.FreqMapAggregator;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractAccumuloElementConverterTest<T extends AccumuloElementConverter> {

    protected T converter;

    @Before
    public void setUp() throws SchemaException, IOException {
        final Schema schema = Schema.fromJson(StreamUtil.schemas(getClass()));
        converter = createConverter(schema);
    }

    protected abstract T createConverter(final Schema schema);

    //TEST WE CAN RETRIEVE AN ELEMENT FROM A KEY THAT HAS BEEN CREATED CORRECTLY
    @Test
    public void shouldReturnAccumuloKeyConverterFromBasicEdge() throws SchemaException, IOException {
        // Given
        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .dest("2")
                .source("1")
                .directed(true).build();

        // When
        final Pair<Key, Key> keys = converter.getKeysFromElement(edge);

        // Then
        final Edge newEdge = (Edge) converter.getElementFromKey(keys.getFirst(), false);
        assertEquals("1", newEdge.getSource());
        assertEquals("2", newEdge.getDestination());
        assertEquals(true, newEdge.isDirected());
    }

    @Test
    public void shouldReturnAccumuloKeyConverterFromBasicEntity() throws SchemaException, IOException {
        // Given
        final Entity entity = new Entity(TestGroups.ENTITY);
        entity.setVertex("3");

        // When
        final Key key = converter.getKeyFromEntity(entity);

        // Then
        final Entity newEntity = (Entity) converter.getElementFromKey(key, false);
        assertEquals("3", newEntity.getVertex());
    }

    @Test
    public void shouldReturnAccumuloKeyConverterFromCFCQPropertyEdge() throws SchemaException, IOException {
        // Given
        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .dest("2")
                .source("1")
                .directed(false)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 100)
                .build();

        // When
        final Pair<Key, Key> keys = converter.getKeysFromElement(edge);
        final Edge newEdge = (Edge) converter.getElementFromKey(keys.getFirst(), false);

        // Then
        assertEquals("1", newEdge.getSource());
        assertEquals("2", newEdge.getDestination());
        assertEquals(false, newEdge.isDirected());
        assertEquals(100, newEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldReturnAccumuloKeyConverterFromCFCQPropertyEntity() throws SchemaException, IOException {
        // Given
        final Entity entity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("3")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 100)
                .build();

        // When
        final Pair<Key, Key> keys = converter.getKeysFromElement(entity);
        final Entity newEntity = (Entity) converter.getElementFromKey(keys.getFirst(), false);

        // Then
        assertEquals("3", newEntity.getVertex());
        assertEquals(100, newEntity.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldReturnAccumuloKeyConverterMultipleCQPropertyEdge() throws SchemaException, IOException {
        // Given
        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .dest("2")
                .source("1")
                .directed(true)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 100)
                .build();

        // When
        final Pair<Key, Key> keys = converter.getKeysFromElement(edge);
        final Edge newEdge = (Edge) converter.getElementFromKey(keys.getSecond(), true);

        // Then
        assertEquals("1", newEdge.getSource());
        assertEquals("2", newEdge.getDestination());
        assertEquals(true, newEdge.isDirected());
        assertEquals(100, newEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
        assertEquals(EdgeId.MatchedVertex.DESTINATION, newEdge.getMatchedVertex());
    }

    @Test
    public void shouldReturnAccumuloKeyConverterMultipleCQPropertiesEntity() throws SchemaException, IOException {
        // Given
        final Entity entity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("3")
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 100)
                .build();

        // When
        final Pair<Key, Key> keys = converter.getKeysFromElement(entity);
        final Entity newEntity = (Entity) converter.getElementFromKey(keys.getFirst(), false);

        // Then
        assertEquals("3", newEntity.getVertex());
        assertEquals(100, newEntity.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldGetOriginalEdge() throws SchemaException, IOException {
        // Given
        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .dest("2")
                .source("1")
                .directed(true)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, 100)
                .build();

        final Pair<Key, Key> keys = converter.getKeysFromElement(edge);

        // When
        final Edge newEdge = (Edge) converter.getElementFromKey(keys.getSecond(), false);

        // Then
        assertEquals("1", newEdge.getSource());
        assertEquals("2", newEdge.getDestination());
        assertEquals(true, newEdge.isDirected());
        assertEquals(100, newEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldSkipNullPropertyValuesWhenCreatingAccumuloKey() throws SchemaException, IOException {
        // Given
        final Edge edge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("1")
                .dest("2")
                .directed(true)
                .property(AccumuloPropertyNames.COLUMN_QUALIFIER, null)
                .build();

        // When
        final Pair<Key, Key> keys = converter.getKeysFromElement(edge);
        Properties properties = converter.getPropertiesFromColumnQualifier(TestGroups.EDGE, keys.getFirst().getColumnQualifierData().getBackingArray());

        // Then
        assertEquals(null, properties.get(AccumuloPropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldSerialiseAndDeSerialiseBetweenPropertyAndValue() {
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
    public void shouldSerialiseAndDeSerialiseBetweenPropertyAndValueMissingMiddleProperty() {
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
    public void shouldSerialiseAndDeSerialiseBetweenPropertyAndValueMissingEndProperty() {
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
    public void shouldSerialiseAndDeSerialiseBetweenPropertyAndValueMissingStartProperty() {
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
    public void shouldSerialiseAndDeSerialiseBetweenPropertyAndValueWithNullProperty() {
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
    public void shouldTruncatePropertyBytes() {
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
        final BytesAndRange br = converter.getPropertiesAsBytesFromColumnQualifier(TestGroups.EDGE, bytes, 2);

        // Then
        final Properties truncatedProperties = new Properties() {
            {
                put(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
                put(AccumuloPropertyNames.COLUMN_QUALIFIER_2, 2);
            }
        };
        byte[] truncatedBytes = new byte[br.getLength()];
        System.arraycopy(bytes, br.getOffSet(), truncatedBytes, 0, br.getLength());
        assertEquals(truncatedProperties, converter.getPropertiesFromColumnQualifier(TestGroups.EDGE, truncatedBytes));
    }

    @Test
    public void shouldTruncatePropertyBytesWithEmptyBytes() {
        // Given
        final byte[] bytes = AccumuloStoreConstants.EMPTY_BYTES;

        // When
        final BytesAndRange truncatedBytes = converter.getPropertiesAsBytesFromColumnQualifier(TestGroups.EDGE, bytes, 2);

        // Then
        assertEquals(0, truncatedBytes.getLength());
    }

    @Test
    public void shouldBuildTimestampFromProperty() {
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
    public void shouldBuildTimestampFromDefaultTimeWhenPropertyIsNull() {
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
    public void shouldBuildTimestampFromDefaultTime() {
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
    public void shouldGetPropertiesFromTimestamp() {
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

        final long timestamp = System.currentTimeMillis();
        final String group = TestGroups.EDGE;

        // When
        final Properties properties = converter.getPropertiesFromTimestamp(group, timestamp);

        // Then
        assertEquals(1, properties.size());
        assertEquals(timestamp, properties.get(AccumuloPropertyNames.TIMESTAMP));
    }

    @Test
    public void shouldGetEmptyPropertiesFromTimestampWhenNoTimestampPropertyInGroup() {
        // Given
        // add timestamp property name but don't add the property to the edge group
        final Schema schema = new Schema.Builder()
                .json(StreamUtil.schemas(getClass()))
                .build();
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
    public void shouldGetEmptyPropertiesFromTimestampWhenNoTimestampProperty() {
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
    public void shouldSerialiseAndDeserialisePropertiesWhenAllAreEmpty() {
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

    @Test
    public void shouldDeserialiseEntityId() {
        // Given 
        final EntityId expectedElementId = new EntitySeed("vertex1");
        final Entity entity = new Entity.Builder()
                .vertex("vertex1")
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, new FreqMap())
                .property(TestPropertyNames.PROP_2, new FreqMap())
                .build();
        final Key key = converter.getKeyFromEntity(entity);

        // When
        final ElementId elementId = converter.getElementId(key, false);

        // Then
        assertEquals(expectedElementId, elementId);
    }

    @Test
    public void shouldDeserialiseEdgeId() {
        // Given 
        final EdgeId expectedElementId = new EdgeSeed("source1", "dest1", true);
        final Edge edge = new Edge.Builder()
                .source("source1")
                .dest("dest1")
                .directed(true)
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, new FreqMap())
                .property(TestPropertyNames.PROP_2, new FreqMap())
                .build();
        final Key key = converter.getKeysFromEdge(edge).getFirst();

        // When
        final ElementId elementId = converter.getElementId(key, false);

        // Then
        assertEquals(expectedElementId, elementId);
    }

    @Test
    public void shouldDeserialiseEdgeIdWithQueriedDestVertex() {
        // Given 
        final EdgeId expectedElementId = new EdgeSeed("vertex1", "vertex2", true, EdgeId.MatchedVertex.DESTINATION);
        final Edge edge = new Edge.Builder()
                .source("vertex1")
                .dest("vertex2")
                .directed(true)
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, new FreqMap())
                .property(TestPropertyNames.PROP_2, new FreqMap())
                .build();

        final Key key = converter.getKeysFromEdge(edge).getSecond();

        // When
        final ElementId elementId = converter.getElementId(key, false);

        // Then
        assertEquals(expectedElementId, elementId);
    }

    @Test
    public void shouldDeserialiseEdgeIdWithQueriedSourceVertex() {
        // Given 
        final EdgeId expectedElementId = new EdgeSeed("source1", "dest1", true);
        final Edge edge = new Edge.Builder()
                .source("source1")
                .dest("dest1")
                .directed(true)
                .group(TestGroups.ENTITY)
                .property(TestPropertyNames.PROP_1, new FreqMap())
                .property(TestPropertyNames.PROP_2, new FreqMap())
                .build();

        final Key key = converter.getKeysFromEdge(edge).getSecond();

        // When
        final ElementId elementId = converter.getElementId(key, false);

        // Then
        assertEquals(expectedElementId, elementId);
    }
}