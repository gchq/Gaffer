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

package gaffer.data.elementdefinition.schema;

import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementAggregator;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.TypedElementDefinitionValidator;
import gaffer.data.elementdefinition.TypedElementDefinitionValidatorTest;
import gaffer.function.AggregateFunction;
import gaffer.function.context.PassThroughFunctionContext;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class DataElementDefinitionValidatorTest extends TypedElementDefinitionValidatorTest {

    @Test
    public void shouldValidateAndReturnTrueWhenNoPropertiesAggregated() {
        // Given
        final DataElementDefinition elementDef = mock(DataElementDefinition.class);
        final DataElementDefinitionValidator validator = new DataElementDefinitionValidator();
        given(elementDef.getIdentifiers()).willReturn(new HashSet<IdentifierType>());
        given(elementDef.getProperties()).willReturn(new HashSet<String>());
        given(elementDef.getValidator()).willReturn(mock(ElementFilter.class));
        given(elementDef.getAggregator()).willReturn(mock(ElementAggregator.class));

        // When
        final boolean isValid = validator.validate(elementDef);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateAndReturnTrueWhenAggregatorIsValid() {
        // Given
        final DataElementDefinition elementDef = mock(DataElementDefinition.class);
        final DataElementDefinitionValidator validator = new DataElementDefinitionValidator();
        final ElementAggregator aggregator = mock(ElementAggregator.class);
        final PassThroughFunctionContext<ElementComponentKey, AggregateFunction> context1 = mock(PassThroughFunctionContext.class);
        final AggregateFunction function = mock(AggregateFunction.class);
        final ElementComponentKey key1 = new ElementComponentKey(TestPropertyNames.F1);
        final ElementComponentKey key2 = new ElementComponentKey(TestPropertyNames.F2);
        final List<PassThroughFunctionContext<ElementComponentKey, AggregateFunction>> contexts = new ArrayList<>();
        contexts.add(context1);

        given(elementDef.getIdentifiers()).willReturn(new HashSet<IdentifierType>());
        given(elementDef.getProperties()).willReturn(new HashSet<>(Arrays.asList(TestPropertyNames.F1, TestPropertyNames.F2)));
        given(elementDef.getValidator()).willReturn(mock(ElementFilter.class));
        given(elementDef.getAggregator()).willReturn(aggregator);
        given(context1.getSelection()).willReturn(Arrays.asList(key1, key2));
        given(function.getInputClasses()).willReturn(new Class[]{String.class, Integer.class});
        given(context1.getFunction()).willReturn(function);
        given(aggregator.getFunctions()).willReturn(contexts);
        given(elementDef.getPropertyClass(TestPropertyNames.F1)).willReturn((Class) String.class);
        given(elementDef.getPropertyClass(TestPropertyNames.F2)).willReturn((Class) Integer.class);
        given(elementDef.getClass(key1)).willReturn((Class) String.class);
        given(elementDef.getClass(key2)).willReturn((Class) Integer.class);

        // When
        final boolean isValid = validator.validate(elementDef);

        // Then
        assertTrue(isValid);
        verify(elementDef).getClass(key1);
        verify(elementDef).getClass(key2);
        verify(function).getInputClasses();
    }

    @Test
    public void shouldValidateAndReturnFalseWhenAPropertyDoesNotHaveAnAggregatorFunction() {
        // Given
        final DataElementDefinition elementDef = mock(DataElementDefinition.class);
        final DataElementDefinitionValidator validator = new DataElementDefinitionValidator();
        final ElementAggregator aggregator = mock(ElementAggregator.class);
        final PassThroughFunctionContext<ElementComponentKey, AggregateFunction> context1 = mock(PassThroughFunctionContext.class);
        final AggregateFunction function = mock(AggregateFunction.class);
        final ElementComponentKey key1 = new ElementComponentKey(TestPropertyNames.F1);
        final List<PassThroughFunctionContext<ElementComponentKey, AggregateFunction>> contexts = new ArrayList<>();
        contexts.add(context1);

        given(elementDef.getIdentifiers()).willReturn(new HashSet<IdentifierType>());
        given(elementDef.getProperties()).willReturn(new HashSet<>(Arrays.asList(TestPropertyNames.F1, TestPropertyNames.F2)));
        given(elementDef.getValidator()).willReturn(mock(ElementFilter.class));
        given(elementDef.getAggregator()).willReturn(aggregator);
        given(context1.getSelection()).willReturn(Collections.singletonList(key1));
        given(function.getInputClasses()).willReturn(new Class[]{String.class, Integer.class});
        given(context1.getFunction()).willReturn(function);
        given(aggregator.getFunctions()).willReturn(contexts);
        given(elementDef.getPropertyClass(TestPropertyNames.F1)).willReturn((Class) String.class);
        given(elementDef.getPropertyClass(TestPropertyNames.F2)).willReturn((Class) Integer.class);

        // When
        final boolean isValid = validator.validate(elementDef);

        // Then
        assertFalse(isValid);
    }

    @Override
    protected TypedElementDefinitionValidator newElementDefinitionValidator() {
        return new DataElementDefinitionValidator();
    }
}