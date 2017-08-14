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
import uk.gov.gchq.gaffer.commonutil.TestGroups;
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

import static org.junit.Assert.assertEquals;
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
            assertTrue(result.getErrorString().contains("reserved word"));
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
        assertEquals("Validation errors: \nClass null for property property1 could not be found", result.getErrorString());
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
        assertTrue(result.getErrorString().contains("null function"));
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
        assertEquals("Validation errors: \nControl value class java.lang.Integer is not compatible" +
                " with the input type: class java.lang.String", result.getErrorString());

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
    public void shouldValidateAndReturnTrueWhenAggregationIsDisabled() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
        final Map<String, String> properties = new HashMap<>();
        properties.put(TestPropertyNames.PROP_1, "int");
        properties.put(TestPropertyNames.PROP_2, "string");
        final BinaryOperator<Integer> function1 = mock(BinaryOperator.class);
        final ElementAggregator aggregator = new ElementAggregator.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(function1)
                .build();

        given(elementDef.getIdentifiers()).willReturn(new HashSet<>());
        given(elementDef.getProperties()).willReturn(properties.keySet());
        given(elementDef.getPropertyMap()).willReturn(properties);
        given(elementDef.getValidator()).willReturn(mock(ElementFilter.class));
        given(elementDef.getFullAggregator()).willReturn(aggregator);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_1)).willReturn((Class) Integer.class);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_2)).willReturn((Class) String.class);
        given(elementDef.isAggregate()).willReturn(false);

        // When
        final ValidationResult result = validator.validate(elementDef);

        // Then
        assertTrue(result.isValid());
    }

    @Test
    public void shouldValidateAndReturnTrueWhenAggregatorIsValid() {
        // Given
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
        final BinaryOperator<Integer> function1 = mock(BinaryOperator.class);
        final BinaryOperator function2 = mock(BinaryOperator.class);
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("int1")
                        .property(TestPropertyNames.PROP_1, "int1")
                        .property(TestPropertyNames.PROP_2, "int2")
                        .build())
                .type("int1", new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .aggregateFunction(function1)
                        .build())
                .type("int2", new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .aggregateFunction(function2)
                        .build())
                .build();

        // When
        final ValidationResult result = validator.validate(schema.getEntity(TestGroups.ENTITY));

        // Then
        assertTrue(result.isValid());
    }

    @Test
    public void shouldValidateAndReturnFalseWhenAggregatorIsInvalid() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
        final Map<String, String> properties = new HashMap<>();
        properties.put(TestPropertyNames.PROP_1, "int");
        properties.put(TestPropertyNames.PROP_2, "string");
        final BinaryOperator<Integer> function1 = mock(BinaryOperator.class);
        final ElementAggregator aggregator = new ElementAggregator.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(function1)
                .build();

        given(elementDef.getIdentifiers()).willReturn(new HashSet<>());
        given(elementDef.getProperties()).willReturn(properties.keySet());
        given(elementDef.getPropertyMap()).willReturn(properties);
        given(elementDef.getValidator()).willReturn(mock(ElementFilter.class));
        given(elementDef.getFullAggregator()).willReturn(aggregator);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_1)).willReturn((Class) Integer.class);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_2)).willReturn((Class) String.class);
        given(elementDef.isAggregate()).willReturn(true);

        // When
        final ValidationResult result = validator.validate(elementDef);

        // Then
        assertFalse(result.isValid());
        assertTrue(result.getErrorString().contains("No aggregator found for properties"));
        verify(elementDef, Mockito.atLeastOnce()).getPropertyClass(TestPropertyNames.PROP_1);
        verify(elementDef, Mockito.atLeastOnce()).getPropertyClass(TestPropertyNames.PROP_2);
    }

    @Test
    public void shouldValidateAndReturnFalseWhenNoAggregatorByGroupBysSet() {
        // Given
        Set<String> groupBys = new HashSet<>();
        groupBys.add("int");
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
        final Map<String, String> properties = new HashMap<>();
        properties.put(TestPropertyNames.PROP_1, "int");
        properties.put(TestPropertyNames.PROP_2, "string");

        given(elementDef.getGroupBy()).willReturn(groupBys);
        given(elementDef.getProperties()).willReturn(properties.keySet());
        given(elementDef.getPropertyMap()).willReturn(properties);
        given(elementDef.getValidator()).willReturn(mock(ElementFilter.class));
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_1)).willReturn((Class) Integer.class);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_2)).willReturn((Class) String.class);
        given(elementDef.isAggregate()).willReturn(false);

        // When
        final ValidationResult result = validator.validate(elementDef);

        // Then
        assertFalse(result.isValid());
        assertEquals("Validation errors: \nGroups with aggregation disabled should not have groupBy properties.", result.getErrorString());
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
        given(elementDef.getFullAggregator()).willReturn(null);
        given(elementDef.isAggregate()).willReturn(true);

        // When
        final ValidationResult result = validator.validate(elementDef);

        // Then
        assertTrue(result.isValid());
    }

    @Test
    public void shouldValidateAndReturnFalseWhenAggregatorProvidedWithNoProperties() {
        // Given
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
        final BinaryOperator<Integer> function1 = mock(BinaryOperator.class);
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("id")
                        .aggregator(new ElementAggregator.Builder()
                                .select(IdentifierType.VERTEX.name())
                                .execute(function1)
                                .build())
                        .build())
                .type("id", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .build();

        // When
        final ValidationResult result = validator.validate(schema.getElement(TestGroups.ENTITY));

        // Then
        assertFalse(result.isValid());
        assertEquals(com.google.common.collect.Sets.newHashSet("Groups with no properties should not have any aggregators"),
                result.getErrors());
    }

    @Test
    public void shouldValidateAndReturnFalseWhenAggregatorHasIdentifierInSelection() {
        // Given
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
        final BinaryOperator<Integer> function1 = mock(BinaryOperator.class);
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("id")
                        .property(TestPropertyNames.STRING, "string")
                        .aggregator(new ElementAggregator.Builder()
                                .select(IdentifierType.VERTEX.name())
                                .execute(function1)
                                .build())
                        .build())
                .type("id", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(function1)
                        .build())
                .build();

        // When
        final ValidationResult result = validator.validate(schema.getElement(TestGroups.ENTITY));

        // Then
        assertFalse(result.isValid());
        assertEquals(com.google.common.collect.Sets.newHashSet("Identifiers cannot be selected for aggregation: " + IdentifierType.VERTEX.name()),
                result.getErrors());
    }

    @Test
    public void shouldValidateAndReturnFalseWhenVisibilityIsAggregatedWithOtherProperty() {
        // Given
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
        final BinaryOperator<Integer> function1 = mock(BinaryOperator.class);
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("id")
                        .property(TestPropertyNames.STRING, "string")
                        .property(TestPropertyNames.VISIBILITY, "string")
                        .aggregator(new ElementAggregator.Builder()
                                .select(TestPropertyNames.STRING, TestPropertyNames.VISIBILITY)
                                .execute(function1)
                                .build())
                        .build())
                .visibilityProperty(TestPropertyNames.VISIBILITY)
                .type("id", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(function1)
                        .build())
                .build();

        // When
        final ValidationResult result = validator.validate(schema.getElement(TestGroups.ENTITY));

        // Then
        assertFalse(result.isValid());
        assertEquals(com.google.common.collect.Sets.newHashSet(
                "The visibility property must be aggregated by itself. It is currently aggregated in the tuple: [stringProperty, visibility], by aggregate function: " + function1.getClass().getName()
                ),
                result.getErrors());
    }

    @Test
    public void shouldValidateAndReturnTrueWhenTimestampIsAggregatedWithANonGroupByProperty() {
        // Given
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
        final BinaryOperator<Integer> function1 = mock(BinaryOperator.class);
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("id")
                        .property(TestPropertyNames.STRING, "string")
                        .property(TestPropertyNames.TIMESTAMP, "string")
                        .aggregator(new ElementAggregator.Builder()
                                .select(TestPropertyNames.STRING, TestPropertyNames.TIMESTAMP)
                                .execute(function1)
                                .build())
                        .build())
                .timestampProperty(TestPropertyNames.TIMESTAMP)
                .type("id", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(function1)
                        .build())
                .build();

        // When
        final ValidationResult result = validator.validate(schema.getElement(TestGroups.ENTITY));

        // Then
        assertTrue(result.isValid());
    }

    @Test
    public void shouldValidateAndReturnFalseWhenTimestampIsAggregatedWithAGroupByProperty() {
        // Given
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
        final BinaryOperator<Integer> function1 = mock(BinaryOperator.class);
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("id")
                        .property(TestPropertyNames.STRING, "string")
                        .property(TestPropertyNames.TIMESTAMP, "string")
                        .groupBy(TestPropertyNames.STRING)
                        .aggregator(new ElementAggregator.Builder()
                                .select(TestPropertyNames.STRING, TestPropertyNames.TIMESTAMP)
                                .execute(function1)
                                .build())
                        .build())
                .timestampProperty(TestPropertyNames.TIMESTAMP)
                .type("id", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(function1)
                        .build())
                .build();

        // When
        final ValidationResult result = validator.validate(schema.getElement(TestGroups.ENTITY));

        // Then
        assertFalse(result.isValid());
        assertEquals(com.google.common.collect.Sets.newHashSet(
                "groupBy properties and non-groupBy properties (including timestamp) must be not be aggregated using the same BinaryOperator. Selection tuple: [stringProperty, timestamp], is aggregated by: " + function1.getClass().getName()
                ),
                result.getErrors());
    }

    @Test
    public void shouldValidateAndReturnFalseWhenGroupByIsAggregatedWithNonGroupByProperty() {
        // Given
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
        final BinaryOperator<Integer> function1 = mock(BinaryOperator.class);
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("id")
                        .property(TestPropertyNames.PROP_1, "string")
                        .property(TestPropertyNames.PROP_2, "string")
                        .groupBy(TestPropertyNames.PROP_1)
                        .aggregator(new ElementAggregator.Builder()
                                .select(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                                .execute(function1)
                                .build())
                        .build())
                .type("id", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(function1)
                        .build())
                .build();

        // When
        final ValidationResult result = validator.validate(schema.getElement(TestGroups.ENTITY));

        // Then
        assertFalse(result.isValid());
        assertEquals(com.google.common.collect.Sets.newHashSet(
                "groupBy properties and non-groupBy properties (including timestamp) must be not be aggregated using the same BinaryOperator. Selection tuple: [property1, property2], is aggregated by: " + function1.getClass().getName()
                ),
                result.getErrors());
    }

    @Test
    public void shouldValidateAndReturnFalseWhenAggregatorHasNoFunction() {
        // Given
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
        final BinaryOperator<Integer> function1 = mock(BinaryOperator.class);
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("id")
                        .property(TestPropertyNames.PROP_1, "string")
                        .aggregator(new ElementAggregator.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(null)
                                .build())
                        .build())
                .type("id", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .build();

        // When
        final ValidationResult result = validator.validate(schema.getElement(TestGroups.ENTITY));

        // Then
        assertFalse(result.isValid());
        assertEquals(com.google.common.collect.Sets.newHashSet(
                "ElementAggregator contains a null function."
                ),
                result.getErrors());
    }

    @Test
    public void shouldValidateAndReturnFalseWhenAggregatorSelectionHasUnknownProperty() {
        // Given
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
        final BinaryOperator<Integer> function1 = mock(BinaryOperator.class);
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("id")
                        .property(TestPropertyNames.STRING, "string")
                        .aggregator(new ElementAggregator.Builder()
                                .select(TestPropertyNames.STRING, TestPropertyNames.PROP_1)
                                .execute(function1)
                                .build())
                        .build())
                .type("id", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(function1)
                        .build())
                .build();

        // When
        final ValidationResult result = validator.validate(schema.getElement(TestGroups.ENTITY));

        // Then
        assertFalse(result.isValid());
        assertEquals(com.google.common.collect.Sets.newHashSet(
                "Unknown property used in an aggregator: " + TestPropertyNames.PROP_1
                ),
                result.getErrors());
    }
}