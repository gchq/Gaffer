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

package gaffer.operation.handler;

import gaffer.data.element.Element;
import gaffer.data.generator.ElementGenerator;
import gaffer.operation.OperationException;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.store.Store;
import gaffer.store.operation.handler.GenerateObjectsHandler;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class GenerateObjectsHandlerTest {

    @Test
    public void shouldReturnObjects() throws OperationException {
        // Given
        final GenerateObjectsHandler<String> handler = new GenerateObjectsHandler<>();
        final Store store = mock(Store.class);
        final GenerateObjects<Element, String> operation = mock(GenerateObjects.class);
        final Iterable<Element> elements = mock(Iterable.class);
        final ElementGenerator<String> elementGenerator = mock(ElementGenerator.class);
        final Iterable<String> objs = mock(Iterable.class);

        given(elementGenerator.getObjects(elements)).willReturn(objs);
        given(operation.getElements()).willReturn(elements);
        given(operation.getElementGenerator()).willReturn(elementGenerator);

        // When
        final Iterable<String> result = handler.doOperation(operation, store);

        // Then
        assertSame(objs, result);
    }
}