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

package uk.gov.gchq.gaffer.store.operation.handler.generate;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.ElementGenerator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class GenerateElementsHandlerTest {

    @Test
    public void shouldReturnElements() throws OperationException {
        // Given
        final GenerateElementsHandler<String> handler = new GenerateElementsHandler<>();
        final Store store = mock(Store.class);
        final GenerateElements<String> operation = mock(GenerateElements.class);
        final CloseableIterable<Element> elements = mock(CloseableIterable.class);
        final ElementGenerator<String> elementGenerator = mock(ElementGenerator.class);
        final CloseableIterable<String> objs = mock(CloseableIterable.class);
        final Context context = new Context();

        final CloseableIterator<Element> elementsIter = mock(CloseableIterator.class);
        given(elements.iterator()).willReturn(elementsIter);
        given(elementGenerator.getElements(objs)).willReturn(elements);
        given(operation.getObjects()).willReturn(objs);
        given(operation.getElementGenerator()).willReturn(elementGenerator);

        // When
        final CloseableIterable<Element> result = handler.doOperation(operation, context, store);

        // Then
        assertSame(elementsIter, result.iterator());
    }
}