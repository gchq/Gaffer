/*
 * Copyright 2016-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class ValidatedElementsTest {

    private List<Element> elements;

    private List<ElementFilter> filters;

    @Mock
    private Schema schema;

    @BeforeEach
    public void setup() {
        elements = new ArrayList<>();
        filters = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            elements.add(mock(Element.class));
            filters.add(mock(ElementFilter.class));

            final String group = "group " + i;
            lenient().when(elements.get(i).getGroup()).thenReturn(group);
            lenient().when(filters.get(i).test(elements.get(i))).thenReturn(true);
            lenient().when(filters.get(i).testWithValidationResult(elements.get(i))).thenReturn(new ValidationResult());

            final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
            lenient().when(schema.getElement(group)).thenReturn(elementDef);
            lenient().when(elementDef.getValidator(true)).thenReturn(filters.get(i));
        }
        lenient().when(filters.get(1).test(elements.get(1))).thenReturn(false);
        lenient().when(filters.get(1).testWithValidationResult(elements.get(1))).thenReturn(new ValidationResult("Some error"));
    }

    @Test
    public void shouldCreateIteratorThatReturnsOnlyValidElements() {
        // Given
        final boolean skipInvalidElements = true;

        final ValidatedElements validElements = new ValidatedElements(elements, schema, skipInvalidElements);
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
        final ValidatedElements validElements = new ValidatedElements(elements, schema, skipInvalidElements);
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
        assertThatIllegalArgumentException()
                .isThrownBy(() -> itr.hasNext())
                .withMessageContaining("Some error");

        verify(filters.get(2), never()).test(elements.get(2));
    }


    @Test
    public void shouldThrowExceptionIfNextCalledWhenNoNextElement() {
        // Given
        final boolean skipInvalidElements = true;
        final ValidatedElements validElements = new ValidatedElements(elements, schema, skipInvalidElements);
        final Iterator<Element> itr = validElements.iterator();

        // When 1
        final Element next0 = itr.next();
        final Element next1 = itr.next();

        // Then 1
        assertSame(elements.get(0), next0);
        assertSame(elements.get(2), next1);

        // When 2 / Then 2
        assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> itr.next()).extracting("message").isNotNull();
    }

    @Test
    public void shouldThrowExceptionIfRemoveCalled() {
        // Given
        final boolean skipInvalidElements = true;
        final ValidatedElements validElements = new ValidatedElements(elements, schema, skipInvalidElements);
        final Iterator<Element> itr = validElements.iterator();

        // When / Then
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> itr.remove()).extracting("message").isNotNull();
    }

    @Test
    public void shouldNotThrowStackOverflowExceptionOnSkipInvalid() {
        final Set<Element> x = new HashSet<>();
        for (int i = 0; i < 10000; i++) {
            x.add(new Entity("G", "" + i));
        }
        final ValidatedElements ve = new ValidatedElements(x, new View.Builder().build(), true);
        Iterator<Element> it = ve.iterator();
        try {
            while (it.hasNext()) {
                it.next();
            }
        } catch (final StackOverflowError ex) {
            fail("Unexpected StackOverflowError.");
        }
    }
}
