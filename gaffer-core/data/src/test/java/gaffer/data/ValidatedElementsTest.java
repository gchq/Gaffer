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

package gaffer.data;

import gaffer.data.element.Element;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.schema.DataElementDefinition;
import gaffer.data.elementdefinition.schema.DataSchema;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ValidatedElementsTest {
    private List<Element> elements;
    private List<ElementFilter> filters;
    private DataSchema dataSchema;

    @Before
    public void setup() {
        elements = new ArrayList<>();
        filters = new ArrayList<>();
        dataSchema = mock(DataSchema.class);

        for (int i = 0; i < 3; i++) {
            elements.add(mock(Element.class));
            filters.add(mock(ElementFilter.class));

            final String group = "group " + i;
            given(elements.get(i).getGroup()).willReturn(group);
            given(filters.get(i).filter(elements.get(i))).willReturn(true);

            final DataElementDefinition elementDef = mock(DataElementDefinition.class);
            given(dataSchema.getElement(group)).willReturn(elementDef);
            given(elementDef.getValidator()).willReturn(filters.get(i));
        }
        given(filters.get(1).filter(elements.get(1))).willReturn(false);
    }

    @Test
    public void shouldCreateIteratorThatReturnsOnlyValidElements() {
        // Given
        final boolean skipInvalidElements = true;

        final ValidatedElements validElements = new ValidatedElements(elements, dataSchema, skipInvalidElements);
        final Iterator<Element> itr = validElements.iterator();

        // When 1a
        final boolean hasNext1 = itr.hasNext();

        // Then 1a
        assertTrue(hasNext1);

        // When 1b
        final Element next1 = itr.next();

        // Then 1b
        assertSame(elements.get(0), next1);

        // When 2a / Then 2a
        final boolean hasNext2 = itr.hasNext();

        // Then 2a
        assertTrue(hasNext2);

        // When 2b
        final Element next2 = itr.next();

        // Then 2b
        assertSame(elements.get(2), next2);
    }

    @Test
    public void shouldCreateIteratorThatThrowsExceptionOnInvalidElement() {
        // Given
        final boolean skipInvalidElements = false;
        final ValidatedElements validElements = new ValidatedElements(elements, dataSchema, skipInvalidElements);
        final Iterator<Element> itr = validElements.iterator();

        // When 1a
        final boolean hasNext1 = itr.hasNext();

        // Then 1a
        assertTrue(hasNext1);

        // When 1b
        final Element next1 = itr.next();

        // Then 1b
        assertSame(elements.get(0), next1);

        // When 2a / Then 2a
        try {
            itr.hasNext();
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
        }

        verify(filters.get(2), never()).filter(elements.get(2));
    }


    @Test
    public void shouldThrowExceptionIfNextCalledWhenNoNextElement() {
        // Given
        final boolean skipInvalidElements = true;
        final ValidatedElements validElements = new ValidatedElements(elements, dataSchema, skipInvalidElements);
        final Iterator<Element> itr = validElements.iterator();

        // When 1
        final Element next0 = itr.next();
        final Element next1 = itr.next();

        // Then 1
        assertSame(elements.get(0), next0);
        assertSame(elements.get(2), next1);

        // When 2 / Then 2
        try {
            itr.next();
            fail("Exception expected");
        } catch (NoSuchElementException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldThrowExceptionIfRemoveCalled() {
        // Given
        final boolean skipInvalidElements = true;
        final ValidatedElements validElements = new ValidatedElements(elements, dataSchema, skipInvalidElements);
        final Iterator<Element> itr = validElements.iterator();

        // When / Then
        try {
            itr.remove();
            fail("Exception expected");
        } catch (UnsupportedOperationException e) {
            assertNotNull(e);
        }
    }
}
