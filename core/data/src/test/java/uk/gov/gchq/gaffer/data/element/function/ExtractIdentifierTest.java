/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.data.element.function;

import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.IdentifierType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ExtractIdentifierTest {

    @Test
    public void shouldReturnNullForNullElement() {
        // Given
        final ExtractIdentifier extractor = new ExtractIdentifier();

        // When
        final Object result = extractor.apply(null);

        // Then
        assertNull(result);
    }

    @Test
    public void shouldReturnNullWithNoIdentifierTypeProvided() {
        // Given
        final Element element = mock(Element.class);

        final ExtractIdentifier extractor = new ExtractIdentifier();

        // When
        final Object result = extractor.apply(element);

        // Then
        assertNull(result);
    }

    @Test
    public void shouldReturnNullWhenIdentifierTypeNotFoundInElement() {
        // Given
        final Element element = mock(Element.class);

        final IdentifierType type = IdentifierType.VERTEX;

        final ExtractIdentifier extractor = new ExtractIdentifier(type);

        // When
        final Object result = extractor.apply(element);

        // Then
        assertNull(result);
    }

    @Test
    public void shouldReturnValueOfIdentifierType() {
        // Given
        final Element element = mock(Element.class);
        final IdentifierType type = IdentifierType.SOURCE;
        final String value = "testSource";

        final ExtractIdentifier extractor = new ExtractIdentifier(type);

        given(element.getIdentifier(type)).willReturn(value);

        // When
        final Object result = extractor.apply(element);

        // Then
        assertEquals(value, result);
    }
}
