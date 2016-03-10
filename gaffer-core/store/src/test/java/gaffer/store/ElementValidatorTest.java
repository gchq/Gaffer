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

package gaffer.store;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import gaffer.commonutil.TestGroups;
import gaffer.data.element.Element;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.store.schema.SchemaElementDefinition;
import gaffer.store.schema.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ElementValidatorTest {

    @Test
    public void shouldReturnTrueWhenSchemaValidateWithValidElement() {
        // Given
        final Schema schema = mock(Schema.class);
        final String group = TestGroups.EDGE;
        final Element elm = mock(Element.class);
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final ElementFilter filter = mock(ElementFilter.class);
        final boolean includeIsA = true;
        final ElementValidator validator = new ElementValidator(schema, includeIsA);

        given(elm.getGroup()).willReturn(group);
        given(schema.getElement(group)).willReturn(elementDef);
        given(elementDef.getValidator(includeIsA)).willReturn(filter);
        given(filter.filter(elm)).willReturn(true);

        // When
        final boolean isValid = validator.validate(elm);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldReturnTrueWhenSchemaValidateWithoutIsAWithValidElement() {
        // Given
        final Schema schema = mock(Schema.class);
        final String group = TestGroups.EDGE;
        final Element elm = mock(Element.class);
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final ElementFilter filter = mock(ElementFilter.class);
        final boolean includeIsA = false;
        final ElementValidator validator = new ElementValidator(schema, includeIsA);

        given(elm.getGroup()).willReturn(group);
        given(schema.getElement(group)).willReturn(elementDef);
        given(elementDef.getValidator(includeIsA)).willReturn(filter);
        given(filter.filter(elm)).willReturn(true);

        // When
        final boolean isValid = validator.validate(elm);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldReturnFalseWhenSchemaValidateWithInvalidElement() {
        // Given
        final Schema schema = mock(Schema.class);
        final String group = TestGroups.EDGE;
        final Element elm = mock(Element.class);
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final ElementFilter filter = mock(ElementFilter.class);
        final boolean includeIsA = true;
        final ElementValidator validator = new ElementValidator(schema, includeIsA);

        given(elm.getGroup()).willReturn(group);
        given(schema.getElement(group)).willReturn(elementDef);
        given(elementDef.getValidator(includeIsA)).willReturn(filter);
        given(filter.filter(elm)).willReturn(false);

        // When
        final boolean isValid = validator.validate(elm);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldReturnFalseWhenNoSchemaElementDefinition() {
        // Given
        final Schema schema = mock(Schema.class);
        final String group = TestGroups.EDGE;
        final Element elm = mock(Element.class);
        final boolean includeIsA = true;
        final ElementValidator validator = new ElementValidator(schema, includeIsA);

        given(elm.getGroup()).willReturn(group);
        given(schema.getElement(group)).willReturn(null);

        // When
        final boolean isValid = validator.validate(elm);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldReturnTrueWhenViewValidateWithValidElement() {
        // Given
        final View view = mock(View.class);
        final String group = TestGroups.EDGE;
        final Element elm = mock(Element.class);
        final ViewElementDefinition elementDef = mock(ViewElementDefinition.class);
        final ElementFilter filter = mock(ElementFilter.class);
        final ElementValidator validator = new ElementValidator(view);

        given(elm.getGroup()).willReturn(group);
        given(view.getElement(group)).willReturn(elementDef);
        given(elementDef.getFilter()).willReturn(filter);
        given(filter.filter(elm)).willReturn(true);

        // When
        final boolean isValid = validator.validate(elm);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldReturnFalseWhenViewValidateWithInvalidElement() {
        // Given
        final View view = mock(View.class);
        final String group = TestGroups.EDGE;
        final Element elm = mock(Element.class);
        final ViewElementDefinition elementDef = mock(ViewElementDefinition.class);
        final ElementFilter filter = mock(ElementFilter.class);
        final ElementValidator validator = new ElementValidator(view);

        given(elm.getGroup()).willReturn(group);
        given(view.getElement(group)).willReturn(elementDef);
        given(elementDef.getFilter()).willReturn(filter);
        given(filter.filter(elm)).willReturn(false);

        // When
        final boolean isValid = validator.validate(elm);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldReturnFalseWhenNoViewElementDefinition() {
        // Given
        final View view = mock(View.class);
        final String group = TestGroups.EDGE;
        final Element elm = mock(Element.class);
        final ElementValidator validator = new ElementValidator(view);

        given(elm.getGroup()).willReturn(group);
        given(view.getElement(group)).willReturn(null);

        // When
        final boolean isValid = validator.validate(elm);

        // Then
        assertFalse(isValid);
    }
}
