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

package gaffer.data.elementdefinition;

import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.IdentifierType;
import gaffer.function.ConsumerFunction;
import gaffer.function.context.ConsumerFunctionContext;
import gaffer.function.processor.Processor;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public abstract class TypedElementDefinitionValidatorTest {
    @Test
    public void shouldValidateComponentTypesAndReturnTrueWhenNoIdentifiersOrProperties() {
        // Given
        final TypedElementDefinition elementDef = mock(TypedElementDefinition.class);
        final TypedElementDefinitionValidator validator = newElementDefinitionValidator();
        given(elementDef.getIdentifiers()).willReturn(new HashSet<IdentifierType>());
        given(elementDef.getProperties()).willReturn(new HashSet<String>());

        // When
        final boolean isValid = validator.validateComponentTypes(elementDef);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateComponentTypesAndReturnTrueWhenIdentifiersAndPropertiesHaveClasses() {
        // Given
        final TypedElementDefinition elementDef = mock(TypedElementDefinition.class);
        final TypedElementDefinitionValidator validator = newElementDefinitionValidator();
        given(elementDef.getIdentifiers()).willReturn(Sets.newSet(IdentifierType.DESTINATION, IdentifierType.SOURCE));
        given(elementDef.getProperties()).willReturn(Sets.newSet(TestPropertyNames.F1, TestPropertyNames.F2));

        given(elementDef.getIdentifierClass(IdentifierType.DESTINATION)).willReturn((Class) Double.class);
        given(elementDef.getIdentifierClass(IdentifierType.SOURCE)).willReturn((Class) Long.class);
        given(elementDef.getPropertyClass(TestPropertyNames.F1)).willReturn((Class) Integer.class);
        given(elementDef.getPropertyClass(TestPropertyNames.F2)).willReturn((Class) String.class);

        // When
        final boolean isValid = validator.validateComponentTypes(elementDef);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateComponentTypesAndReturnFalseForInvalidPropertyClass() {
        // Given
        final TypedElementDefinition elementDef = mock(TypedElementDefinition.class);
        final TypedElementDefinitionValidator validator = newElementDefinitionValidator();
        given(elementDef.getIdentifiers()).willReturn(new HashSet<IdentifierType>());
        given(elementDef.getProperties()).willReturn(Sets.newSet(TestPropertyNames.F1));
        given(elementDef.getPropertyClass(TestPropertyNames.F1)).willThrow(new IllegalArgumentException());

        // When
        final boolean isValid = validator.validateComponentTypes(elementDef);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldValidateFunctionSelectionsAndReturnFalseWhenAFunctionIsNull() {
        // Given
        final TypedElementDefinition elementDef = mock(TypedElementDefinition.class);
        final Processor<ElementComponentKey, ConsumerFunctionContext<ElementComponentKey, ConsumerFunction>> processor = mock(Processor.class);
        final TypedElementDefinitionValidator validator = newElementDefinitionValidator();

        final List<ConsumerFunctionContext<ElementComponentKey, ConsumerFunction>> functions = new ArrayList<>();
        final ConsumerFunctionContext<ElementComponentKey, ConsumerFunction> function = mock(ConsumerFunctionContext.class);
        functions.add(function);
        given(function.getFunction()).willReturn(null);

        given(processor.getFunctions()).willReturn(functions);

        // When
        final boolean isValid = validator.validateFunctionArgumentTypes(processor, elementDef);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldValidateFunctionSelectionsAndReturnTrueWhenNoFunctionsSet() {
        // Given
        final TypedElementDefinition elementDef = mock(TypedElementDefinition.class);
        final Processor<ElementComponentKey, ConsumerFunctionContext<ElementComponentKey, ConsumerFunction>> processor = mock(Processor.class);
        final TypedElementDefinitionValidator validator = newElementDefinitionValidator();

        given(processor.getFunctions()).willReturn(null);

        // When
        final boolean isValid = validator.validateFunctionArgumentTypes(processor, elementDef);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateFunctionSelectionsAndReturnTrueWhenProcessorIsNull() {
        // Given
        final TypedElementDefinition elementDef = mock(TypedElementDefinition.class);
        final Processor<ElementComponentKey, ConsumerFunctionContext<ElementComponentKey, ConsumerFunction>> processor = null;
        final TypedElementDefinitionValidator validator = newElementDefinitionValidator();

        // When
        final boolean isValid = validator.validateFunctionArgumentTypes(processor, elementDef);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateFunctionSelectionsAndReturnFalseWhenFunctionTypeDoesNotEqualSelectionType() {
        // Given
        final TypedElementDefinition elementDef = mock(TypedElementDefinition.class);
        final Processor<ElementComponentKey, ConsumerFunctionContext<ElementComponentKey, ConsumerFunction>> processor = mock(Processor.class);
        final TypedElementDefinitionValidator validator = newElementDefinitionValidator();

        final List<ConsumerFunctionContext<ElementComponentKey, ConsumerFunction>> functions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final ConsumerFunctionContext<ElementComponentKey, ConsumerFunction> context = mock(ConsumerFunctionContext.class);
            final ConsumerFunction function = mock(ConsumerFunction.class);
            given(function.getInputClasses()).willReturn(new Class<?>[]{String.class});

            given(context.getFunction()).willReturn(function);
            given(context.getSelection()).willReturn(
                    Collections.singletonList(new ElementComponentKey("key" + i + "a")));
            functions.add(context);

            given(elementDef.getClass(context.getSelection().get(0))).willReturn((Class) Integer.class);
        }

        given(processor.getFunctions()).willReturn(functions);

        // When
        final boolean isValid = validator.validateFunctionArgumentTypes(processor, elementDef);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldValidateFunctionSelectionsAndReturnTrueWhenAllFunctionsAreValid() {
        // Given
        final TypedElementDefinition elementDef = mock(TypedElementDefinition.class);
        final Processor<ElementComponentKey, ConsumerFunctionContext<ElementComponentKey, ConsumerFunction>> processor = mock(Processor.class);
        final TypedElementDefinitionValidator validator = newElementDefinitionValidator();

        final List<ConsumerFunctionContext<ElementComponentKey, ConsumerFunction>> functions = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final ConsumerFunctionContext<ElementComponentKey, ConsumerFunction> context = mock(ConsumerFunctionContext.class);
            final ConsumerFunction function = mock(ConsumerFunction.class);
            given(function.getInputClasses()).willReturn(new Class<?>[]{CharSequence.class, CharSequence.class});

            given(context.getFunction()).willReturn(function);
            given(context.getSelection()).willReturn(
                    Arrays.asList(new ElementComponentKey("key" + i + "a"), new ElementComponentKey("key" + i + "b")));
            functions.add(context);

            given(elementDef.getClass(context.getSelection().get(0))).willReturn((Class) CharSequence.class);
            given(elementDef.getClass(context.getSelection().get(1))).willReturn((Class) String.class);
        }

        given(processor.getFunctions()).willReturn(functions);

        // When
        final boolean isValid = validator.validateFunctionArgumentTypes(processor, elementDef);

        // Then
        assertTrue(isValid);
    }

    protected abstract TypedElementDefinitionValidator newElementDefinitionValidator();
}