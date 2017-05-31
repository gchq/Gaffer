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

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.collections.Sets;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SchemaElementDefinitionValidatorTest {
    @Test
    public void shouldValidateComponentTypesAndReturnTrueWhenNoIdentifiersOrProperties() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

        given(elementDef.getIdentifiers()).willReturn(new HashSet<>());
        given(elementDef.getProperties()).willReturn(new HashSet<>());

        // When
        final ValidationResult result = validator.validateComponentTypes(elementDef);

        // Then
        assertTrue(result.isValid());
    }

    @Test
    public void shouldValidateComponentTypesAndErrorWhenPropertyNameIsAReservedWord() {
        for (final IdentifierType identifierType : IdentifierType.values()) {
            // Given
            final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
            final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
            final Set<String> properties = new HashSet<>();
            properties.add(TestPropertyNames.COUNT);
            properties.add(identifierType.name());

            given(elementDef.getIdentifiers()).willReturn(new HashSet<>());
            given(elementDef.getProperties()).willReturn(properties);

            // When
            final ValidationResult result = validator.validateComponentTypes(elementDef);

            // Then
            assertFalse(result.isValid());
        }
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
        final ValidationResult result = validator.validateComponentTypes(elementDef);

        // Then
        assertTrue(result.isValid());
    }

    @Test
    public void shouldValidateComponentTypesAndReturnFalseForInvalidPropertyClass() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

        given(elementDef.getIdentifiers()).willReturn(new HashSet<>());
        given(elementDef.getProperties()).willReturn(Sets.newSet(TestPropertyNames.PROP_1));
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_1)).willThrow(new IllegalArgumentException());

        // When
        final ValidationResult result = validator.validateComponentTypes(elementDef);

        // Then
        assertFalse(result.isValid());
    }

    @Test
    public void shouldValidateFunctionSelectionsAndReturnFalseWhenAFunctionIsNull() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final ElementFilter elementFilter = new ElementFilter.Builder()
                .select("selection")
                .execute(null)
                .build();
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

        // When
        final ValidationResult result = validator.validateFunctionArgumentTypes(elementFilter, elementDef);

        // Then
        assertFalse(result.isValid());
    }

    @Test
    public void shouldValidateFunctionSelectionsAndReturnTrueWhenNoFunctionsSet() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final ElementFilter elementFilter = new ElementFilter();
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

        // When
        final ValidationResult result = validator.validateFunctionArgumentTypes(elementFilter, elementDef);

        // Then
        assertTrue(result.isValid());
    }

    @Test
    public void shouldValidateFunctionSelectionsAndReturnTrueWhenElementFilterIsNull() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final ElementFilter elementFilter = null;
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

        // When
        final ValidationResult result = validator.validateFunctionArgumentTypes(elementFilter, elementDef);

        // Then
        assertTrue(result.isValid());
    }

    @Test
    public void shouldValidateFunctionSelectionsAndReturnFalseWhenFunctionTypeDoesNotEqualSelectionType() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        given(elementDef.getPropertyClass("selection")).willReturn((Class) String.class);

        final IsMoreThan function = new IsMoreThan(5);
        final ElementFilter elementFilter = new ElementFilter.Builder()
                .select("selection")
                .execute(function)
                .build();
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

        // When
        final ValidationResult result = validator.validateFunctionArgumentTypes(elementFilter, elementDef);

        // Then
        assertFalse(result.isValid());
    }

    @Test
    public void shouldValidateFunctionSelectionsAndReturnTrueWhenAllFunctionsAreValid() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        given(elementDef.getPropertyClass("selectionStr")).willReturn((Class) String.class);
        given(elementDef.getPropertyClass("selectionInt")).willReturn((Class) Integer.class);

        final Predicate<String> function1 = a -> a.contains("true");
        final Predicate<Integer> function2 = a -> a > 0;
        final ElementFilter elementFilter = new ElementFilter.Builder()
                .select("selectionStr")
                .execute(function1)
                .select("selectionInt")
                .execute(function2)
                .build();
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

        // When
        final ValidationResult result = validator.validateFunctionArgumentTypes(elementFilter, elementDef);

        // Then
        assertTrue(result.isValid());
    }

    @Test
    public void shouldValidateAndReturnTrueWhenNoPropertiesAggregated() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

        given(elementDef.getIdentifiers()).willReturn(new HashSet<>());
        given(elementDef.getProperties()).willReturn(new HashSet<>());
        given(elementDef.getValidator()).willReturn(mock(ElementFilter.class));
        given(elementDef.getAggregator()).willReturn(mock(ElementAggregator.class));

        // When
        final ValidationResult result = validator.validate(elementDef, false);

        // Then
        assertTrue(result.isValid());
    }

    @Test
    public void shouldValidateAndReturnTrueWhenAggregatorIsValid() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
        final Map<String, String> properties = new HashMap<>();
        properties.put(TestPropertyNames.PROP_1, "int");
        properties.put(TestPropertyNames.PROP_2, "int");
        final BinaryOperator<Integer> function1 = mock(BinaryOperator.class);
        final BinaryOperator function2 = mock(BinaryOperator.class);
        final ElementAggregator aggregator = new ElementAggregator.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(function1)
                .select(TestPropertyNames.PROP_2)
                .execute(function2)
                .build();

        given(elementDef.getIdentifiers()).willReturn(new HashSet<>());
        given(elementDef.getProperties()).willReturn(properties.keySet());
        given(elementDef.getPropertyMap()).willReturn(properties);
        given(elementDef.getValidator()).willReturn(mock(ElementFilter.class));
        given(elementDef.getAggregator()).willReturn(aggregator);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_1)).willReturn((Class) Integer.class);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_2)).willReturn((Class) Integer.class);

        // When
        final ValidationResult result = validator.validate(elementDef, true);

        // Then
        assertTrue(result.isValid());
        verify(elementDef, Mockito.atLeastOnce()).getPropertyClass(TestPropertyNames.PROP_1);
        verify(elementDef, Mockito.atLeastOnce()).getPropertyClass(TestPropertyNames.PROP_2);
    }

    @Test
    public void shouldValidateAndReturnTrueWhenNoPropertiesSoAggregatorIsValid() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

        given(elementDef.getIdentifiers()).willReturn(new HashSet<>());
        given(elementDef.getPropertyMap()).willReturn(Collections.emptyMap());
        given(elementDef.getValidator()).willReturn(mock(ElementFilter.class));
        given(elementDef.getAggregator()).willReturn(null);

        // When
        final ValidationResult result = validator.validate(elementDef, true);

        // Then
        assertTrue(result.isValid());
    }

    @Test
    public void shouldValidateAndReturnFalseWhenNoBinaryOperatorAndBinaryOperatorsAreRequired() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

        given(elementDef.getIdentifiers()).willReturn(new HashSet<>());
        final Map<String, String> properties = new HashMap<>();
        properties.put(TestPropertyNames.PROP_1, "string");
        properties.put(TestPropertyNames.PROP_2, "int");
        given(elementDef.getPropertyMap()).willReturn(properties);
        given(elementDef.getProperties()).willReturn(properties.keySet());
        given(elementDef.getValidator()).willReturn(mock(ElementFilter.class));
        given(elementDef.getAggregator()).willReturn(null);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_1)).willReturn((Class) String.class);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_2)).willReturn((Class) Integer.class);

        // When
        final ValidationResult result = validator.validate(elementDef, true);

        // Then
        assertFalse(result.isValid());
    }

    @Test
    public void shouldValidateAndReturnFalseWhenAPropertyDoesNotHaveAnBinaryOperator() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
        final Map<String, String> properties = new HashMap<>();
        properties.put(TestPropertyNames.PROP_1, "string");
        properties.put(TestPropertyNames.PROP_2, "int");
        final BinaryOperator<Integer> function1 = mock(BinaryOperator.class);
        final ElementAggregator aggregator = new ElementAggregator.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(function1)
                .build();

        given(elementDef.getIdentifiers()).willReturn(new HashSet<>());
        given(elementDef.getProperties()).willReturn(properties.keySet());
        given(elementDef.getPropertyMap()).willReturn(properties);
        given(elementDef.getValidator()).willReturn(mock(ElementFilter.class));
        given(elementDef.getAggregator()).willReturn(aggregator);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_1)).willReturn((Class) String.class);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_2)).willReturn((Class) Integer.class);

        // When
        final ValidationResult result = validator.validate(elementDef, true);

        // Then
        assertFalse(result.isValid());
    }
}