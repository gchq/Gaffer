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
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.schema.DataElementDefinition;
import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.operation.OperationException;
import gaffer.operation.impl.Validate;
import gaffer.store.Store;
import gaffer.store.operation.handler.ValidateHandler;
import java.util.Collections;
import java.util.Iterator;
import org.junit.Test;

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

        // When
        final Iterable<Element> result = handler.doOperation(validate, store);

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
        final Iterable<Element> elements = Collections.singletonList(elm1);
        final DataSchema dataSchema = mock(DataSchema.class);

        given(validate.getElements()).willReturn(elements);
        given(validate.isSkipInvalidElements()).willReturn(false);
        given(store.getDataSchema()).willReturn(dataSchema);
        final String group = "group";
        given(elm1.getGroup()).willReturn(group);
        final DataElementDefinition elementDef = mock(DataElementDefinition.class);
        final ElementFilter validator = mock(ElementFilter.class);
        given(validator.filter(elm1)).willReturn(true);
        given(elementDef.getValidator()).willReturn(validator);
        given(dataSchema.getElement(group)).willReturn(elementDef);

        // When
        final Iterable<Element> result = handler.doOperation(validate, store);

        // Then
        final Iterator<Element> itr = result.iterator();
        final Element elm1Result = itr.next();
        assertSame(elm1, elm1Result);
        assertFalse(itr.hasNext());
    }
}