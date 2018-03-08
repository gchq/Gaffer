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
package uk.gov.gchq.gaffer.mapstore.factory;

import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.mapstore.multimap.MapOfSets;
import uk.gov.gchq.gaffer.mapstore.utils.ElementCloner;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SimpleMapFactoryTest {

    @Test
    public void shouldThrowExceptionIfMapClassIsInvalid() throws StoreException {
        // Given
        final Class mapClass = String.class;
        final Schema schema = mock(Schema.class);
        final MapStoreProperties properties = mock(MapStoreProperties.class);
        final SimpleMapFactory factory = new SimpleMapFactory();

        given(properties.get(SimpleMapFactory.MAP_CLASS, SimpleMapFactory.MAP_CLASS_DEFAULT)).willReturn(mapClass.getName());

        // When / Then
        try {
            factory.initialise(schema, properties);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldExtractMapClassFromPropertiesWhenInitialised() throws StoreException {
        // Given
        final Class<? extends Map> mapClass = LinkedHashMap.class;
        final Schema schema = mock(Schema.class);
        final MapStoreProperties properties = mock(MapStoreProperties.class);
        final SimpleMapFactory factory = new SimpleMapFactory();

        given(properties.get(SimpleMapFactory.MAP_CLASS, SimpleMapFactory.MAP_CLASS_DEFAULT)).willReturn(mapClass.getName());

        // When
        factory.initialise(schema, properties);

        // Then
        assertEquals(mapClass, factory.getMapClass());
    }

    @Test
    public void shouldCreateNewMapUsingMapClass() throws StoreException {
        // Given
        final Class<? extends Map> mapClass = LinkedHashMap.class;
        final Schema schema = mock(Schema.class);
        final MapStoreProperties properties = mock(MapStoreProperties.class);
        final SimpleMapFactory factory = new SimpleMapFactory();

        given(properties.get(SimpleMapFactory.MAP_CLASS, SimpleMapFactory.MAP_CLASS_DEFAULT)).willReturn(mapClass.getName());

        factory.initialise(schema, properties);

        // When
        final Map<Object, Object> map1 = factory.getMap("mapName1", Object.class, Object.class);
        final Map<Object, Object> map2 = factory.getMap("mapName2", Object.class, Object.class);

        // Then
        assertTrue(map1.isEmpty());
        assertTrue(map1 instanceof LinkedHashMap);
        assertTrue(map2.isEmpty());
        assertTrue(map2 instanceof LinkedHashMap);
        assertNotSame(map1, map2);
    }

    @Test
    public void shouldThrowExceptionIfMapClassCannotBeInstantiated() throws StoreException {
        // Given
        final Class<? extends Map> mapClass = Map.class;
        final Schema schema = mock(Schema.class);
        final MapStoreProperties properties = mock(MapStoreProperties.class);
        final SimpleMapFactory factory = new SimpleMapFactory();

        given(properties.get(SimpleMapFactory.MAP_CLASS, SimpleMapFactory.MAP_CLASS_DEFAULT)).willReturn(mapClass.getName());

        factory.initialise(schema, properties);

        // When / Then
        try {
            factory.getMap("mapName1", Object.class, Object.class);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldCreateNewMultiMap() throws StoreException {
        // Given
        final Class<? extends Map> mapClass = LinkedHashMap.class;
        final Schema schema = mock(Schema.class);
        final MapStoreProperties properties = mock(MapStoreProperties.class);
        final SimpleMapFactory factory = new SimpleMapFactory();

        given(properties.get(SimpleMapFactory.MAP_CLASS, SimpleMapFactory.MAP_CLASS_DEFAULT)).willReturn(mapClass.getName());

        factory.initialise(schema, properties);

        // When
        final MapOfSets<Object, Object> map1 = (MapOfSets) factory.getMultiMap("mapName1", Object.class, Object.class);
        final MapOfSets<Object, Object> map2 = (MapOfSets) factory.getMultiMap("mapName2", Object.class, Object.class);

        // Then
        assertTrue(map1.getWrappedMap().isEmpty());
        assertTrue(map1.getWrappedMap().isEmpty());
        assertTrue(map2.getWrappedMap() instanceof LinkedHashMap);
        assertTrue(map2.getWrappedMap() instanceof LinkedHashMap);
        assertNotSame(map1, map2);
    }

    @Test
    public void shouldCloneElementUsingCloner() throws StoreException {
        // Given
        final ElementCloner elementCloner = mock(ElementCloner.class);
        final Element element = mock(Element.class);
        final Element expectedClonedElement = mock(Element.class);
        final Schema schema = mock(Schema.class);
        final SimpleMapFactory factory = new SimpleMapFactory(elementCloner);

        given(elementCloner.cloneElement(element, schema)).willReturn(expectedClonedElement);

        // When
        final Element clonedElement = factory.cloneElement(element, schema);

        // Then
        verify(elementCloner).cloneElement(element, schema);
        assertSame(expectedClonedElement, clonedElement);
        assertNotSame(element, clonedElement);
    }
}
