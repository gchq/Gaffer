/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.mapstore.impl;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.GroupedProperties;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.mapstore.factory.MapFactory;
import uk.gov.gchq.gaffer.mapstore.multimap.MultiMap;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Map;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class MapImplTest {
    private static MapFactory mockMapFactory;

    @Before
    public void before() {
        mockMapFactory = mock(MapFactory.class);
    }

    @After
    public void after() {
        mockMapFactory = null;
    }

    @Test
    public void shouldCreateMapsUsingMapFactory() throws StoreException {
        // Given
        final Schema schema = mock(Schema.class);
        final MapStoreProperties properties = mock(MapStoreProperties.class);
        final Map aggElements = mock(Map.class);
        final Map nonAggElements = mock(Map.class);
        final MultiMap entityIdToElements = mock(MultiMap.class);
        final MultiMap edgeIdToElements = mock(MultiMap.class);

        given(schema.getGroups()).willReturn(Sets.newHashSet(TestGroups.EDGE));
        given(properties.getMapFactory()).willReturn(TestMapFactory.class.getName());
        given(properties.getCreateIndex()).willReturn(true);
        given(mockMapFactory.getMap(TestGroups.EDGE + "|" + MapImpl.AGG_ELEMENTS, Element.class, GroupedProperties.class)).willReturn(aggElements);
        given(mockMapFactory.getMap(TestGroups.EDGE + "|" + MapImpl.NON_AGG_ELEMENTS, Element.class, Integer.class)).willReturn(nonAggElements);
        given(mockMapFactory.getMultiMap(MapImpl.ENTITY_ID_TO_ELEMENTS, EntityId.class, Element.class)).willReturn(entityIdToElements);
        given(mockMapFactory.getMultiMap(MapImpl.EDGE_ID_TO_ELEMENTS, EdgeId.class, Element.class)).willReturn(edgeIdToElements);

        // When
        new MapImpl(schema, properties);

        // Then
        verify(mockMapFactory).getMap(TestGroups.EDGE + "|" + MapImpl.AGG_ELEMENTS, Element.class, GroupedProperties.class);
        verify(mockMapFactory).getMap(TestGroups.EDGE + "|" + MapImpl.NON_AGG_ELEMENTS, Element.class, Long.class);
        verify(mockMapFactory).getMultiMap(MapImpl.ENTITY_ID_TO_ELEMENTS, EntityId.class, Element.class);
        verify(mockMapFactory).getMultiMap(MapImpl.EDGE_ID_TO_ELEMENTS, EdgeId.class, Element.class);
    }

    @Test
    public void shouldNotCreateIndexesIfNotRequired() throws StoreException {
        // Given
        final Schema schema = mock(Schema.class);
        final MapStoreProperties properties = mock(MapStoreProperties.class);
        final Map aggElements = mock(Map.class);
        final Map nonAggElements = mock(Map.class);

        given(schema.getGroups()).willReturn(Sets.newHashSet(TestGroups.EDGE));
        given(properties.getMapFactory()).willReturn(TestMapFactory.class.getName());
        given(properties.getCreateIndex()).willReturn(false);
        given(mockMapFactory.getMap(TestGroups.EDGE + "|" + MapImpl.AGG_ELEMENTS, Element.class, GroupedProperties.class)).willReturn(aggElements);
        given(mockMapFactory.getMap(TestGroups.EDGE + "|" + MapImpl.NON_AGG_ELEMENTS, Element.class, Integer.class)).willReturn(nonAggElements);

        // When
        new MapImpl(schema, properties);

        // Then
        verify(mockMapFactory).getMap(TestGroups.EDGE + "|" + MapImpl.AGG_ELEMENTS, Element.class, GroupedProperties.class);
        verify(mockMapFactory).getMap(TestGroups.EDGE + "|" + MapImpl.NON_AGG_ELEMENTS, Element.class, Long.class);
        verify(mockMapFactory, never()).getMultiMap(MapImpl.ENTITY_ID_TO_ELEMENTS, EntityId.class, Element.class);
        verify(mockMapFactory, never()).getMultiMap(MapImpl.EDGE_ID_TO_ELEMENTS, EdgeId.class, Element.class);
    }

    public static final class TestMapFactory implements MapFactory {

        @Override
        public void initialise(final Schema schema, final MapStoreProperties properties) {
            mockMapFactory.initialise(schema, properties);
        }

        @Override
        public <K, V> Map<K, V> getMap(final String mapName, final Class<K> keyClass, final Class<V> valueClass) {
            return mockMapFactory.getMap(mapName, keyClass, valueClass);
        }

        @Override
        public <K, V> MultiMap<K, V> getMultiMap(final String mapName, final Class<K> keyClass, final Class<V> valueClass) {
            return mockMapFactory.getMultiMap(mapName, keyClass, valueClass);
        }

        @Override
        public void clear() {
            mockMapFactory.clear();
        }

        @Override
        public Element cloneElement(final Element element, final Schema schema) {
            return mockMapFactory.cloneElement(element, schema);
        }
    }
}
