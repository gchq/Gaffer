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
package gaffer.accumulostore.key.core.impl;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import gaffer.accumulostore.key.AccumuloElementConverter;
import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.accumulostore.utils.AccumuloPropertyNames;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.accumulostore.utils.Pair;
import gaffer.commonutil.StreamUtil;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Edge;
import gaffer.data.element.Entity;
import gaffer.data.element.Properties;
import gaffer.data.elementdefinition.exception.SchemaException;
import gaffer.store.schema.Schema;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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
        edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, "Test");

        // When
        final Pair<Key> keys = converter.getKeysFromElement(edge);
        final Edge newEdge = (Edge) converter.getElementFromKey(keys.getFirst());

        // Then
        assertEquals("1", newEdge.getSource());
        assertEquals("2", newEdge.getDestination());
        assertEquals(false, newEdge.isDirected());
        assertEquals("Test", newEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldReturnAccumuloKeyConverterFromCFCQPropertydEntity() throws SchemaException, AccumuloElementConversionException, IOException {
        // Given
        final Entity entity = new Entity(TestGroups.ENTITY);
        entity.setVertex("3");
        entity.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, new Date());

        // When
        final Pair<Key> keys = converter.getKeysFromElement(entity);
        final Entity newEntity = (Entity) converter.getElementFromKey(keys.getFirst());

        // Then
        assertEquals("3", newEntity.getVertex());
        assertEquals(Date.class, newEntity.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER).getClass());
    }

    @Test
    public void shouldReturnAccumuloKeyConverterMultipleCQPropertydEdge() throws SchemaException, AccumuloElementConversionException, IOException {
        // Given
        final Edge edge = new Edge(TestGroups.EDGE);
        edge.setDestination("2");
        edge.setSource("1");
        edge.setDirected(true);
        edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, "Test");

        // When
        final Pair<Key> keys = converter.getKeysFromElement(edge);
        final Edge newEdge = (Edge) converter.getElementFromKey(keys.getSecond());

        // Then
        assertEquals("1", newEdge.getSource());
        assertEquals("2", newEdge.getDestination());
        assertEquals(true, newEdge.isDirected());
        assertEquals("Test", newEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldReturnAccumuloKeyConverterMultipleCQPropertiesEntity() throws SchemaException, AccumuloElementConversionException, IOException {
        // Given
        final Entity entity = new Entity(TestGroups.ENTITY);
        entity.setVertex("3");
        entity.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, new Date());

        // When
        final Pair<Key> keys = converter.getKeysFromElement(entity);
        final Entity newEntity = (Entity) converter.getElementFromKey(keys.getFirst());

        // Then
        assertEquals("3", newEntity.getVertex());
        assertEquals(Date.class, newEntity.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER).getClass());
    }

    @Test
    public void shouldGetOriginalEdgeWithMatchAsSourceNotSet() throws SchemaException, AccumuloElementConversionException, IOException {
        // Given
        final Edge edge = new Edge(TestGroups.EDGE);
        edge.setDestination("2");
        edge.setSource("1");
        edge.setDirected(true);
        edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, "Test");

        final Pair<Key> keys = converter.getKeysFromElement(edge);
        final Map<String, String> options = new HashMap<>();

        // When
        final Edge newEdge = (Edge) converter.getElementFromKey(keys.getSecond(), options);

        // Then
        assertEquals("1", newEdge.getSource());
        assertEquals("2", newEdge.getDestination());
        assertEquals(true, newEdge.isDirected());
        assertEquals("Test", newEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldGetFlippedEdgeWithMatchAsSourceFalse() throws SchemaException, AccumuloElementConversionException, IOException {
        // Given
        final Edge edge = new Edge(TestGroups.EDGE);
        edge.setDestination("2");
        edge.setSource("1");
        edge.setDirected(true);
        edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, "Test");

        final Pair<Key> keys = converter.getKeysFromElement(edge);
        final Map<String, String> options = new HashMap<>();
        options.put(AccumuloStoreConstants.OPERATION_RETURN_MATCHED_SEEDS_AS_EDGE_SOURCE, "true");

        // When
        final Edge newEdge = (Edge) converter.getElementFromKey(keys.getSecond(), options);

        // Then
        assertEquals("2", newEdge.getSource());
        assertEquals("1", newEdge.getDestination());
        assertEquals(true, newEdge.isDirected());
        assertEquals("Test", newEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void shouldSkipNullPropertyValuesWhenCreatingAccumuloValue() throws SchemaException, AccumuloElementConversionException, IOException {
        // Given
        final Edge edge = new Edge(TestGroups.EDGE);
        edge.setSource("1");
        edge.setDestination("2");
        edge.setDirected(true);
        edge.putProperty(AccumuloPropertyNames.PROP_1, null);
        edge.putProperty(AccumuloPropertyNames.PROP_2, null);
        edge.putProperty(AccumuloPropertyNames.PROP_3, null);
        edge.putProperty(AccumuloPropertyNames.COUNT, null);

        // When
        final Value value = converter.getValueFromElement(edge);

        // Then
        assertEquals(0, value.getSize());
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
}