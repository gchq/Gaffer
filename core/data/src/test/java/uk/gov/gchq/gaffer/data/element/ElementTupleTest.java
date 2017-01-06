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
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ElementTupleTest {

    @Test
    public void shouldSetAndGetFields() {
        // Given
        final Element element1 = mock(Element.class);
        final Element element2 = mock(Element.class);
        final ElementTuple tuple = new ElementTuple(element1);
        tuple.setElement(element1);

        // When / Then
        assertSame(element1, tuple.getElement());
        tuple.setElement(element2);
        assertSame(element2, tuple.getElement());
    }

    @Test
    public void shouldGetIdentifierFromElement() {
        // Given
        final Element element = mock(Element.class);
        final ElementTuple tuple = new ElementTuple(element);
        final IdentifierType idType = IdentifierType.SOURCE;
        final String expectedId = "source";
        given(element.getIdentifier(idType)).willReturn(expectedId);

        // When
        final Object identifier = tuple.get(idType.name());

        // Then
        assertSame(expectedId, identifier);
        verify(element, never()).getProperty(Mockito.anyString());
    }

    @Test
    public void shouldGetPropertyFromElement() {
        // Given
        final Element element = mock(Element.class);
        final ElementTuple tuple = new ElementTuple(element);
        final String propertyName = "property name";
        final String expectedProperty = "property value";
        given(element.getProperty(propertyName)).willReturn(expectedProperty);

        // When
        final Object property = tuple.get(propertyName);

        // Then
        assertSame(expectedProperty, property);
        verify(element, never()).getIdentifier(Mockito.any(IdentifierType.class));
    }

    @Test
    public void shouldPutIdentifierOnElement() {
        // Given
        final Element element = mock(Element.class);
        final ElementTuple tuple = new ElementTuple(element);
        final IdentifierType idType = IdentifierType.SOURCE;
        final String identifier = "source";

        // When
        tuple.put(idType.name(), identifier);

        // Then
        verify(element).putIdentifier(idType, identifier);
        verify(element, never()).putProperty(Mockito.anyString(), Mockito.anyObject());
    }

    @Test
    public void shouldPutPropertyOnElement() {
        // Given
        final Element element = mock(Element.class);
        final ElementTuple tuple = new ElementTuple(element);
        final String propertyName = "property name";
        final String property = "property value";

        // When
        tuple.put(propertyName, property);

        // Then
        verify(element).putProperty(propertyName, property);
        verify(element, never()).putIdentifier(Mockito.any(IdentifierType.class), Mockito.anyObject());
    }
}
