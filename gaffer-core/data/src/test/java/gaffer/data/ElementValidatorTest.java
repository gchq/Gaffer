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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import gaffer.commonutil.TestGroups;
import gaffer.data.element.Element;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.schema.DataElementDefinition;
import gaffer.data.elementdefinition.schema.DataSchema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ElementValidatorTest {

    @Test
    public void shouldReturnTrueWhenValidateWithValidElement() {
        // Given
        final DataSchema dataSchema = mock(DataSchema.class);
        final String group = TestGroups.EDGE;
        final Element elm = mock(Element.class);
        final DataElementDefinition elementDef = mock(DataElementDefinition.class);
        final ElementFilter filter = mock(ElementFilter.class);
        final ElementValidator validator = new ElementValidator(dataSchema);

        given(elm.getGroup()).willReturn(group);
        given(dataSchema.getElement(group)).willReturn(elementDef);
        given(elementDef.getValidator()).willReturn(filter);
        given(filter.filter(elm)).willReturn(true);

        // When
        final boolean isValid = validator.validate(elm);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldReturnFalseWhenValidateWithInvalidElement() {
        // Given
        final DataSchema dataSchema = mock(DataSchema.class);
        final String group = TestGroups.EDGE;
        final Element elm = mock(Element.class);
        final DataElementDefinition elementDef = mock(DataElementDefinition.class);
        final ElementFilter filter = mock(ElementFilter.class);
        final ElementValidator validator = new ElementValidator(dataSchema);

        given(elm.getGroup()).willReturn(group);
        given(dataSchema.getElement(group)).willReturn(elementDef);
        given(elementDef.getValidator()).willReturn(filter);
        given(filter.filter(elm)).willReturn(false);

        // When
        final boolean isValid = validator.validate(elm);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldReturnFalseWhenNoElementDefinition() {
        // Given
        final DataSchema dataSchema = mock(DataSchema.class);
        final String group = TestGroups.EDGE;
        final Element elm = mock(Element.class);
        final ElementValidator validator = new ElementValidator(dataSchema);

        given(elm.getGroup()).willReturn(group);
        given(dataSchema.getElement(group)).willReturn(null);

        // When
        final boolean isValid = validator.validate(elm);

        // Then
        assertFalse(isValid);
    }
}
