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

package uk.gov.gchq.gaffer.store.operation.handler.generate;

import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.ObjectGenerator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.Iterator;
import java.util.function.Function;

import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class GenerateObjectsHandlerTest {

    @Test
    public void shouldReturnObjects() throws OperationException {
        // Given
        final GenerateObjectsHandler<String> handler = new GenerateObjectsHandler<>();
        final Store store = mock(Store.class);
        final GenerateObjects<String> operation = mock(GenerateObjects.class);
        final Iterable elements = mock(Iterable.class);
        final Function<Iterable<? extends Element>, Iterable<? extends String>> objGenerator = mock(ObjectGenerator.class);
        final Iterable objs = mock(Iterable.class);
        final Context context = new Context();
        final Iterator objsIter = mock(Iterator.class);
        given(objs.iterator()).willReturn(objsIter);
        given(objGenerator.apply(elements)).willReturn(objs);
        given(operation.getInput()).willReturn(elements);
        given(operation.getElementGenerator()).willReturn(objGenerator);

        // When
        final Iterable<? extends String> result = handler.doOperation(operation, context, store);

        // Then
        assertSame(objsIter, result.iterator());
    }
}
