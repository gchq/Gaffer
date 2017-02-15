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

package uk.gov.gchq.gaffer.data.generator;

import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.data.TransformIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class OneToOneElementGeneratorTest {

    private Element elm1;
    private Element elm2;

    private final String obj1 = "object 1";
    private final String obj2 = "object 2";

    @Before
    public void setup() {
        elm1 = mock(Element.class);
        elm2 = mock(Element.class);
    }

    @Test
    public void getElementShouldReturnGeneratedElement() {
        // Given
        final OneToOneElementGenerator<String> generator = new OneToOneElementGeneratorImpl();

        // When
        final Element result = generator.getElement(obj1);

        // Then
        assertSame(elm1, result);
    }

    @Test
    public void getObjectShouldReturnGeneratedObject() {
        // Given
        final OneToOneElementGenerator<String> generator = new OneToOneElementGeneratorImpl();

        // When
        final String result = generator.getObject(elm1);

        // Then
        assertSame(obj1, result);
    }

    @Test
    public void getObjectsShouldReturnGeneratedObjectTransformIterable() {
        // Given
        final OneToOneElementGenerator<String> generator = new OneToOneElementGeneratorImpl();

        // When
        final TransformIterable<Element, String> result = (TransformIterable<Element, String>) generator.getObjects(Arrays.asList(elm1, elm2));

        // Then
        final Iterator<String> itr = result.iterator();
        assertSame(obj1, itr.next());
        assertSame(obj2, itr.next());
        assertFalse(itr.hasNext());
    }

    @Test
    public void getElementsShouldReturnGeneratedElementTransformIterable() {
        // Given
        final OneToOneElementGenerator<String> generator = new OneToOneElementGeneratorImpl();

        // When
        final TransformIterable<String, Element> result = (TransformIterable<String, Element>) generator.getElements(Arrays.asList(obj1, obj2));

        // Then
        final Iterator<Element> itr = result.iterator();
        assertSame(elm1, itr.next());
        assertSame(elm2, itr.next());
        assertFalse(itr.hasNext());
    }

    private class OneToOneElementGeneratorImpl extends OneToOneElementGenerator<String> {

        @Override
        public Element getElement(final String item) {
            if (obj1.equals(item)) {
                return elm1;
            }

            if (obj2.equals(item)) {
                return elm2;
            }

            return null;
        }

        @Override
        public String getObject(final Element element) {
            if (elm1 == element) {
                return obj1;
            }

            if (elm2 == element) {
                return obj2;
            }

            return null;
        }
    }
}