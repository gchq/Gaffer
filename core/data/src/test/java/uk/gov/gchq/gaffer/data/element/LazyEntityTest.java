/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.data.element;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class LazyEntityTest {
    @Test
    public void shouldLoadPropertyFromLoader() {
        // Given
        final Entity entity = new Entity("group");
        final ElementValueLoader entityLoader = mock(ElementValueLoader.class);
        final LazyEntity lazyEntity = new LazyEntity(entity, entityLoader);
        final String propertyName = "property name";
        final String exceptedPropertyValue = "property value";
        given(entityLoader.getProperty(propertyName, lazyEntity.getProperties())).willReturn(exceptedPropertyValue);

        // When
        Object propertyValue = lazyEntity.getProperty(propertyName);

        // Then
        assertEquals(exceptedPropertyValue, propertyValue);
    }

    @Test
    public void shouldLoadIdentifierWhenNotLoaded() {
        // Given
        final Entity entity = mock(Entity.class);
        given(entity.getProperties()).willReturn(mock(Properties.class));
        final ElementValueLoader entityLoader = mock(ElementValueLoader.class);
        final LazyEntity lazyEntity = new LazyEntity(entity, entityLoader);
        final IdentifierType identifierType = IdentifierType.VERTEX;
        final String exceptedIdentifierValue = "identifier value";
        given(entity.getVertex()).willReturn(exceptedIdentifierValue);

        // When
        Object identifierValue = lazyEntity.getIdentifier(identifierType);

        // Then
        assertEquals(exceptedIdentifierValue, identifierValue);
        assertEquals(identifierValue, entity.getVertex());
        verify(entityLoader).loadIdentifiers(entity);
    }

    @Test
    public void shouldNotLoadIdentifierWhenLoaded() {
        // Given
        final Entity entity = new Entity("group");
        final ElementValueLoader entityLoader = mock(ElementValueLoader.class);
        final LazyEntity lazyEntity = new LazyEntity(entity, entityLoader);
        final IdentifierType identifierType = IdentifierType.VERTEX;
        final String exceptedIdentifierValue = "identifier value";
        lazyEntity.setVertex(exceptedIdentifierValue);

        // When - should use the loaded value
        Object identifierValue = lazyEntity.getIdentifier(identifierType);
        Object identifierValue2 = lazyEntity.getIdentifier(identifierType);

        // Then
        assertEquals(exceptedIdentifierValue, identifierValue);
        assertEquals(exceptedIdentifierValue, identifierValue2);
        assertEquals(exceptedIdentifierValue, entity.getVertex());
        verify(entityLoader, never()).loadIdentifiers(entity);
    }

    @Test
    public void shouldDelegatePutPropertyToLazyProperties() {
        // Given
        final Entity entity = new Entity("group");
        final ElementValueLoader entityLoader = mock(ElementValueLoader.class);
        final LazyEntity lazyEntity = new LazyEntity(entity, entityLoader);
        final String propertyName = "property name";
        final String propertyValue = "property value";

        // When
        lazyEntity.putProperty(propertyName, propertyValue);

        // Then
        verify(entityLoader, never()).getProperty(propertyName, lazyEntity.getProperties());
        assertEquals(propertyValue, entity.getProperty(propertyName));
        assertEquals(propertyValue, lazyEntity.getProperty(propertyName));
    }

    @Test
    public void shouldDelegateSetIdentifierToEntity() {
        // Given
        final Entity entity = new Entity("group");
        final ElementValueLoader entityLoader = mock(ElementValueLoader.class);

        final LazyEntity lazyEntity = new LazyEntity(entity, entityLoader);
        final String vertex = "vertex";

        // When
        lazyEntity.setVertex(vertex);

        // Then
        verify(entityLoader, never()).loadIdentifiers(entity);
        assertEquals(vertex, entity.getVertex());
    }

    @Test
    public void shouldDelegateGetGroupToEntity() {
        // Given
        final String group = "group";
        final Entity entity = new Entity(group);
        final ElementValueLoader entityLoader = mock(ElementValueLoader.class);
        final LazyEntity lazyEntity = new LazyEntity(entity, entityLoader);

        // When
        final String groupResult = lazyEntity.getGroup();

        // Then
        assertEquals(group, groupResult);
    }

    @Test
    public void shouldGetLazyProperties() {
        // Given
        final Entity entity = new Entity("group");
        final ElementValueLoader entityLoader = mock(ElementValueLoader.class);
        final LazyEntity lazyEntity = new LazyEntity(entity, entityLoader);

        // When
        final LazyProperties result = lazyEntity.getProperties();

        // Then
        assertNotNull(result);
        assertNotSame(entity.getProperties(), result);
    }

    @Test
    public void shouldUnwrapEntity() {
        // Given
        final Entity entity = new Entity("group");
        final ElementValueLoader entityLoader = mock(ElementValueLoader.class);
        final LazyEntity lazyEntity = new LazyEntity(entity, entityLoader);

        // When
        final Entity result = lazyEntity.getElement();

        // Then
        assertSame(entity, result);
    }
}
