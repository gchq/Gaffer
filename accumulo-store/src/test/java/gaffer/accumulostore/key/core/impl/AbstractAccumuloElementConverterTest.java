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


import gaffer.accumulostore.key.AccumuloElementConverter;
import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.accumulostore.utils.AccumuloPropertyNames;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.accumulostore.utils.Pair;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.StreamUtil;
import gaffer.data.element.Edge;
import gaffer.data.element.Entity;
import gaffer.data.elementdefinition.schema.exception.SchemaException;
import gaffer.store.schema.StoreSchema;
import org.apache.accumulo.core.data.Key;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public abstract class AbstractAccumuloElementConverterTest {

    private AccumuloElementConverter converter;

    @Before
    public void setUp() throws SchemaException, IOException {
        StoreSchema storeSchema = StoreSchema.fromJson(StreamUtil.storeSchema(getClass()));
        converter = createConverter(storeSchema);
    }

    protected abstract AccumuloElementConverter createConverter(final StoreSchema storeSchema);

    //TEST WE CAN RETRIEVE AN ELEMENT FROM A KEY THAT HAS BEEN CREATED CORRECTLY
    @Test
    public void TestAccumuloKeyConverterBasicEdge() throws SchemaException, AccumuloElementConversionException, IOException {
        Edge edge = new Edge(TestGroups.EDGE);
        edge.setDestination("2");
        edge.setSource("1");
        edge.setDirected(true);
        Pair<Key> keys = converter.getKeysFromElement(edge);
        Edge newEdge = (Edge) converter.getElementFromKey(keys.getFirst());
        assertEquals("1", newEdge.getSource());
        assertEquals("2", newEdge.getDestination());
        assertEquals(true, newEdge.isDirected());
    }

    @Test
    public void TestAccumuloKeyConverterBasicEntity() throws SchemaException, AccumuloElementConversionException, IOException {
        Entity entity = new Entity(TestGroups.ENTITY);
        entity.setVertex("3");
        Key key = converter.getKeyFromEntity(entity);
        Entity newEntity = (Entity) converter.getElementFromKey(key);
        assertEquals("3", newEntity.getVertex());
    }

    @Test
    public void TestAccumuloKeyConverterCFCQPropertydEdge() throws SchemaException, AccumuloElementConversionException, IOException {
        Edge edge = new Edge(TestGroups.EDGE);
        edge.setDestination("2");
        edge.setSource("1");
        edge.setDirected(false);
        edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, "Test");
        Pair<Key> keys = converter.getKeysFromElement(edge);
        Edge newEdge = (Edge) converter.getElementFromKey(keys.getFirst());
        assertEquals("1", newEdge.getSource());
        assertEquals("2", newEdge.getDestination());
        assertEquals(false, newEdge.isDirected());
        assertEquals("Test", newEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void TestAccumuloKeyConverterCFCQPropertydEntity() throws SchemaException, AccumuloElementConversionException, IOException {
        Entity entity = new Entity(TestGroups.ENTITY);
        entity.setVertex("3");
        entity.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, new Date());
        Pair<Key> keys = converter.getKeysFromElement(entity);
        Entity newEntity = (Entity) converter.getElementFromKey(keys.getFirst());
        assertEquals("3", newEntity.getVertex());
        assertEquals(Date.class, newEntity.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER).getClass());
    }

    @Test
    public void TestAccumuloKeyConverterMultipleCQPropertydEdge() throws SchemaException, AccumuloElementConversionException, IOException {
        Edge edge = new Edge(TestGroups.EDGE);
        edge.setDestination("2");
        edge.setSource("1");
        edge.setDirected(true);
        edge.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, "Test");
        Pair<Key> keys = converter.getKeysFromElement(edge);
        Edge newEdge = (Edge) converter.getElementFromKey(keys.getSecond());
        assertEquals("1", newEdge.getSource());
        assertEquals("2", newEdge.getDestination());
        assertEquals(true, newEdge.isDirected());
        assertEquals("Test", newEdge.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
    }

    @Test
    public void TestAccumuloKeyConverterMultipleCQPropertiesEntity() throws SchemaException, AccumuloElementConversionException, IOException {
        Entity entity = new Entity(TestGroups.ENTITY);
        entity.setVertex("3");
        entity.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, new Date());
        Pair<Key> keys = converter.getKeysFromElement(entity);
        Entity newEntity = (Entity) converter.getElementFromKey(keys.getFirst());
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
}