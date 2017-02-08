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

package uk.gov.gchq.gaffer.data.element;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.collections.Sets;
import org.mockito.runners.MockitoJUnitRunner;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class LazyPropertiesTest {

    @Test
    public void shouldLoadPropertyWhenNotLoaded() {
        // Given
        final ElementValueLoader elementLoader = mock(ElementValueLoader.class);
        final Properties properties = new Properties();
        final String propertyName = "property name";
        final String exceptedPropertyValue = "property value";
        given(elementLoader.getProperty(propertyName)).willReturn(exceptedPropertyValue);

        final LazyProperties lazyProperties = new LazyProperties(properties, elementLoader);

        // When
        final Object propertyValue = lazyProperties.get(propertyName);

        // Then
        assertEquals(exceptedPropertyValue, propertyValue);
        assertEquals(propertyValue, properties.get(propertyName));
    }

    @Test
    public void shouldNotLoadPropertyWhenLoaded() {
        // Given
        final ElementValueLoader elementLoader = mock(ElementValueLoader.class);
        final String propertyName = "property name";
        final String exceptedPropertyValue = "property value";
        final Properties properties = new Properties(propertyName, exceptedPropertyValue);
        final LazyProperties lazyProperties = new LazyProperties(properties, elementLoader);

        given(elementLoader.getProperty(propertyName)).willReturn(exceptedPropertyValue);

        // When
        final Object propertyValue = lazyProperties.get(propertyName);

        // Then
        assertEquals(exceptedPropertyValue, propertyValue);
        verify(elementLoader, never()).getProperty(propertyName);
    }

    @Test
    public void shouldAddPropertyToMapWhenAddingProperty() {
        // Given
        final ElementValueLoader elementLoader = mock(ElementValueLoader.class);
        final Properties properties = new Properties();
        final String propertyName = "property name";
        final String propertyValue = "property value";
        given(elementLoader.getProperty(propertyName)).willReturn(propertyValue);

        final LazyProperties lazyProperties = new LazyProperties(properties, elementLoader);

        // When
        lazyProperties.put(propertyName, propertyValue);

        // Then
        verify(elementLoader, never()).getProperty(propertyName);
        assertEquals(propertyValue, properties.get(propertyName));
    }

    @Test
    public void shouldClearLoadedPropertiesAndMapWhenClearIsCalled() {
        // Given
        final ElementValueLoader elementLoader = mock(ElementValueLoader.class);
        final Properties properties = new Properties();
        final String propertyName = "property name";
        final String propertyValue = "property value";
        given(elementLoader.getProperty(propertyName)).willReturn(propertyValue);

        final LazyProperties lazyProperties = new LazyProperties(properties, elementLoader);
        lazyProperties.get(propertyName); // call it to load the value.

        // When
        lazyProperties.clear();
        lazyProperties.get(propertyName);

        // Then
        verify(elementLoader, times(2)).getProperty(propertyName); // should be called twice before and after clear()
        assertEquals(propertyValue, properties.get(propertyName));
    }

    @Test
    public void shouldRemovePropertyNameFromLoadedPropertiesAndMapWhenRemoveIsCalled() {
        // Given
        final ElementValueLoader elementLoader = mock(ElementValueLoader.class);
        final Properties properties = new Properties();
        final String propertyName = "property name";
        final String propertyValue = "property value";
        given(elementLoader.getProperty(propertyName)).willReturn(propertyValue);

        final LazyProperties lazyProperties = new LazyProperties(properties, elementLoader);
        lazyProperties.get(propertyName); // call it to load the value.

        // When
        lazyProperties.remove(propertyName);
        lazyProperties.get(propertyName);

        // Then
        verify(elementLoader, times(2)).getProperty(propertyName); // should be called twice before and after removeProperty()
        assertEquals(propertyValue, properties.get(propertyName));
    }

    @Test
    public void shouldRemovePropertyNameFromLoadedPropertiesAndMapWhenKeepOnlyThesePropertiesIsCalled() {
        // Given
        final ElementValueLoader elementLoader = mock(ElementValueLoader.class);
        final Properties properties = new Properties();
        final String propertyName1 = "property name1";
        final String propertyName2 = "property name2";
        final String propertyValue1 = "property value1";
        final String propertyValue2 = "property value2";
        given(elementLoader.getProperty(propertyName1)).willReturn(propertyValue1);
        given(elementLoader.getProperty(propertyName2)).willReturn(propertyValue2);

        final LazyProperties lazyProperties = new LazyProperties(properties, elementLoader);
        lazyProperties.get(propertyName1); // call it to load value 1.
        lazyProperties.get(propertyName2); // call it to load value 2.

        // When
        lazyProperties.keepOnly(Sets.newSet(propertyName2));
        lazyProperties.get(propertyName1);
        lazyProperties.get(propertyName2);

        // Then
        verify(elementLoader, times(2)).getProperty(propertyName1);
        verify(elementLoader, times(1)).getProperty(propertyName2);
        assertEquals(propertyValue1, properties.get(propertyName1));
        assertEquals(propertyValue2, properties.get(propertyName2));
    }

    @Test
    public void shouldRemovePropertyNamesFromLoadedPropertiesAndMapWhenRemovePropertiesIsCalled() {
        // Given
        final ElementValueLoader elementLoader = mock(ElementValueLoader.class);
        final Properties properties = new Properties();
        final String propertyName1 = "property name1";
        final String propertyName2 = "property name2";
        final String propertyValue1 = "property value1";
        final String propertyValue2 = "property value2";
        given(elementLoader.getProperty(propertyName1)).willReturn(propertyValue1);
        given(elementLoader.getProperty(propertyName2)).willReturn(propertyValue2);

        final LazyProperties lazyProperties = new LazyProperties(properties, elementLoader);
        lazyProperties.get(propertyName1); // call it to load value 1.
        lazyProperties.get(propertyName2); // call it to load value 2.

        // When
        lazyProperties.remove(Sets.newSet(propertyName1));
        lazyProperties.get(propertyName1);
        lazyProperties.get(propertyName2);

        // Then
        verify(elementLoader, times(2)).getProperty(propertyName1);
        verify(elementLoader, times(1)).getProperty(propertyName2);
        assertEquals(propertyValue1, properties.get(propertyName1));
        assertEquals(propertyValue2, properties.get(propertyName2));
    }

    @Test
    public void shouldDelegateEntrySetMethodToPropertiesInstance() {
        // Given
        final Properties properties = mock(Properties.class);
        final LazyProperties lazyProperties =
                new LazyProperties(properties, null);
        final Set<Map.Entry<String, Object>> expectedEntrySet = mock(Set.class);
        given(properties.entrySet()).willReturn(expectedEntrySet);

        // When
        final Set<Map.Entry<String, Object>> entrySet = lazyProperties.entrySet();

        // Then
        assertSame(expectedEntrySet, entrySet);
    }
}
