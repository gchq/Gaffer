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

package uk.gov.gchq.gaffer.store.operation.handler;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.Validate;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class ValidateHandlerTest {

    @Test
    public void shouldReturnNullIfElementsAreNull() throws OperationException {
        // Given
        final ValidateHandler handler = new ValidateHandler();
        final Store store = mock(Store.class);
        final Validate validate = mock(Validate.class);
        given(validate.getElements()).willReturn(null);
        final Context context = new Context();

        // When
        final Iterable<Element> result = handler.doOperation(validate, context, store);

        // Then
        assertNull(result);
    }

    @Test
    public void shouldValidatedElements() throws OperationException {
        // Given
        final ValidateHandler handler = new ValidateHandler();
        final Store store = mock(Store.class);
        final Validate validate = mock(Validate.class);
        final Element elm1 = mock(Element.class);
        final CloseableIterable<Element> elements = new WrappedCloseableIterable<>(Collections.singletonList(elm1));
        final Schema schema = mock(Schema.class);
        final Context context = new Context();

        given(validate.getElements()).willReturn(elements);
        given(validate.isSkipInvalidElements()).willReturn(false);
        given(store.getSchema()).willReturn(schema);
        final String group = "group";
        given(elm1.getGroup()).willReturn(group);
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final ElementFilter validator = mock(ElementFilter.class);
        given(validator.filter(elm1)).willReturn(true);
        given(elementDef.getValidator(true)).willReturn(validator);
        given(schema.getElement(group)).willReturn(elementDef);

        // When
        final Iterable<Element> result = handler.doOperation(validate, context, store);

        // Then
        final Iterator<Element> itr = result.iterator();
        final Element elm1Result = itr.next();
        assertSame(elm1, elm1Result);
        assertFalse(itr.hasNext());
    }
}