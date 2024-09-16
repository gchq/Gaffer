/*
 * Copyright 2017-2024 Crown Copyright
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

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.mapstore.multimap.MapOfSets;
import uk.gov.gchq.gaffer.mapstore.utils.ElementCloner;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SimpleMapFactoryTest {

    @Test
    void shouldThrowExceptionIfMapClassIsInvalid() {
        // Given
        final Class<String> mapClass = String.class;
        final Schema schema = mock(Schema.class);
        final MapStoreProperties properties = mock(MapStoreProperties.class);
        final SimpleMapFactory factory = new SimpleMapFactory();

        given(properties.get(SimpleMapFactory.MAP_CLASS, SimpleMapFactory.MAP_CLASS_DEFAULT)).willReturn(mapClass.getName());

        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> factory.initialise(schema, properties))
                .extracting("message")
                .isNotNull();
    }

    @Test
    void shouldExtractMapClassFromPropertiesWhenInitialised() {
        // Given
        final Class<? extends Map> mapClass = LinkedHashMap.class;
        final Schema schema = mock(Schema.class);
        final MapStoreProperties properties = mock(MapStoreProperties.class);
        final SimpleMapFactory factory = new SimpleMapFactory();

        given(properties.get(SimpleMapFactory.MAP_CLASS, SimpleMapFactory.MAP_CLASS_DEFAULT)).willReturn(mapClass.getName());

        // When
        factory.initialise(schema, properties);

        // Then
        assertThat(factory.getMapClass()).isEqualTo(mapClass);
    }

    @Test
    void shouldCreateNewMapUsingMapClass() {
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
        assertThat(map1)
                .isInstanceOf(LinkedHashMap.class)
                .isEmpty();
        assertThat(map2)
                .isInstanceOf(LinkedHashMap.class)
                .isEmpty();
        assertThat(map1).isNotSameAs(map2);
    }

    @Test
    void shouldThrowExceptionIfMapClassCannotBeInstantiated() {
        // Given
        final Class<? extends Map> mapClass = Map.class;
        final Schema schema = mock(Schema.class);
        final MapStoreProperties properties = mock(MapStoreProperties.class);
        final SimpleMapFactory factory = new SimpleMapFactory();

        given(properties.get(SimpleMapFactory.MAP_CLASS, SimpleMapFactory.MAP_CLASS_DEFAULT)).willReturn(mapClass.getName());

        factory.initialise(schema, properties);

        // When / Then
        assertThatIllegalArgumentException()
            .isThrownBy(() -> factory.getMap("mapName1", Object.class, Object.class))
            .extracting("message")
            .isNotNull();
    }

    @Test
    void shouldCreateNewMultiMap() {
        // Given
        final Class<? extends Map> mapClass = LinkedHashMap.class;
        final Schema schema = mock(Schema.class);
        final MapStoreProperties properties = mock(MapStoreProperties.class);
        final SimpleMapFactory factory = new SimpleMapFactory();

        given(properties.get(SimpleMapFactory.MAP_CLASS, SimpleMapFactory.MAP_CLASS_DEFAULT)).willReturn(mapClass.getName());

        factory.initialise(schema, properties);

        // When
        final MapOfSets<Object, Object> map1 = (MapOfSets<Object, Object>) factory.getMultiMap("mapName1", Object.class, Object.class);
        final MapOfSets<Object, Object> map2 = (MapOfSets<Object, Object>) factory.getMultiMap("mapName2", Object.class, Object.class);

        // Then
        assertThat(map1.getWrappedMap()).isEmpty();
        assertThat(map1.getWrappedMap()).isEmpty();
        assertThat(map2.getWrappedMap()).isInstanceOf(LinkedHashMap.class);
        assertThat(map2.getWrappedMap()).isInstanceOf(LinkedHashMap.class);
        assertThat(map1).isNotSameAs(map2);
    }

    @Test
    void shouldCloneElementUsingCloner() {
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
        assertThat(clonedElement)
            .isSameAs(expectedClonedElement)
            .isNotSameAs(element);
    }
}
