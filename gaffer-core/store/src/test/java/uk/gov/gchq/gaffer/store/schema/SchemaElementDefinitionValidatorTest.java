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

package uk.gov.gchq.gaffer.store.schema;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.ElementComponentKey;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.function.AggregateFunction;
import uk.gov.gchq.gaffer.function.ConsumerFunction;
import uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext;
import uk.gov.gchq.gaffer.function.context.PassThroughFunctionContext;
import uk.gov.gchq.gaffer.function.processor.Processor;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class SchemaElementDefinitionValidatorTest {
    @Test
    public void shouldValidateComponentTypesAndReturnTrueWhenNoIdentifiersOrProperties() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

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
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

        given(elementDef.getIdentifiers()).willReturn(Sets.newSet(IdentifierType.DESTINATION, IdentifierType.SOURCE));
        given(elementDef.getProperties()).willReturn(Sets.newSet(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2));

        given(elementDef.getIdentifierClass(IdentifierType.DESTINATION)).willReturn((Class) Double.class);
        given(elementDef.getIdentifierClass(IdentifierType.SOURCE)).willReturn((Class) Long.class);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_1)).willReturn((Class) Integer.class);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_2)).willReturn((Class) String.class);

        // When
        final boolean isValid = validator.validateComponentTypes(elementDef);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateComponentTypesAndReturnFalseForInvalidPropertyClass() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

        given(elementDef.getIdentifiers()).willReturn(new HashSet<IdentifierType>());
        given(elementDef.getProperties()).willReturn(Sets.newSet(TestPropertyNames.PROP_1));
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_1)).willThrow(new IllegalArgumentException());

        // When
        final boolean isValid = validator.validateComponentTypes(elementDef);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldValidateFunctionSelectionsAndReturnFalseWhenAFunctionIsNull() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final Processor<ElementComponentKey, ConsumerFunctionContext<ElementComponentKey, ConsumerFunction>> processor = mock(Processor.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

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
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final Processor<ElementComponentKey, ConsumerFunctionContext<ElementComponentKey, ConsumerFunction>> processor = mock(Processor.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

        given(processor.getFunctions()).willReturn(null);

        // When
        final boolean isValid = validator.validateFunctionArgumentTypes(processor, elementDef);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateFunctionSelectionsAndReturnTrueWhenProcessorIsNull() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final Processor<ElementComponentKey, ConsumerFunctionContext<ElementComponentKey, ConsumerFunction>> processor = null;
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

        // When
        final boolean isValid = validator.validateFunctionArgumentTypes(processor, elementDef);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateFunctionSelectionsAndReturnFalseWhenFunctionTypeDoesNotEqualSelectionType() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final Processor<ElementComponentKey, ConsumerFunctionContext<ElementComponentKey, ConsumerFunction>> processor = mock(Processor.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

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
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final Processor<ElementComponentKey, ConsumerFunctionContext<ElementComponentKey, ConsumerFunction>> processor = mock(Processor.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

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

    @Test
    public void shouldValidateAndReturnTrueWhenNoPropertiesAggregated() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

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
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
        final ElementAggregator aggregator = mock(ElementAggregator.class);
        final PassThroughFunctionContext<ElementComponentKey, AggregateFunction> context1 = mock(PassThroughFunctionContext.class);
        final AggregateFunction function = mock(AggregateFunction.class);
        final ElementComponentKey key1 = new ElementComponentKey(TestPropertyNames.PROP_1);
        final ElementComponentKey key2 = new ElementComponentKey(TestPropertyNames.PROP_2);
        final List<PassThroughFunctionContext<ElementComponentKey, AggregateFunction>> contexts = new ArrayList<>();
        contexts.add(context1);

        given(elementDef.getIdentifiers()).willReturn(new HashSet<IdentifierType>());
        given(elementDef.getProperties()).willReturn(new HashSet<>(Arrays.asList(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)));
        given(elementDef.getValidator()).willReturn(mock(ElementFilter.class));
        given(elementDef.getAggregator()).willReturn(aggregator);
        given(context1.getSelection()).willReturn(Arrays.asList(key1, key2));
        given(function.getInputClasses()).willReturn(new Class[]{String.class, Integer.class});
        given(context1.getFunction()).willReturn(function);
        given(aggregator.getFunctions()).willReturn(contexts);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_1)).willReturn((Class) String.class);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_2)).willReturn((Class) Integer.class);
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
    public void shouldValidateAndReturnFalseWhenAPropertyDoesNotHaveAnAggregateFunction() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
        final ElementAggregator aggregator = mock(ElementAggregator.class);
        final PassThroughFunctionContext<ElementComponentKey, AggregateFunction> context1 = mock(PassThroughFunctionContext.class);
        final AggregateFunction function = mock(AggregateFunction.class);
        final ElementComponentKey key1 = new ElementComponentKey(TestPropertyNames.PROP_1);
        final List<PassThroughFunctionContext<ElementComponentKey, AggregateFunction>> contexts = new ArrayList<>();
        contexts.add(context1);

        given(elementDef.getIdentifiers()).willReturn(new HashSet<IdentifierType>());
        given(elementDef.getProperties()).willReturn(new HashSet<>(Arrays.asList(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)));
        given(elementDef.getValidator()).willReturn(mock(ElementFilter.class));
        given(elementDef.getAggregator()).willReturn(aggregator);
        given(context1.getSelection()).willReturn(Collections.singletonList(key1));
        given(function.getInputClasses()).willReturn(new Class[]{String.class, Integer.class});
        given(context1.getFunction()).willReturn(function);
        given(aggregator.getFunctions()).willReturn(contexts);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_1)).willReturn((Class) String.class);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_2)).willReturn((Class) Integer.class);

        // When
        final boolean isValid = validator.validate(elementDef);

        // Then
        assertFalse(isValid);
    }
}