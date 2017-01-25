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

import com.google.common.collect.Sets;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.function.ExampleAggregateFunction;
import uk.gov.gchq.gaffer.function.ExampleFilterFunction;
import uk.gov.gchq.gaffer.function.IsA;
import java.util.Collections;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public abstract class SchemaElementDefinitionTest<T extends SchemaElementDefinition> {
    public static final String PROPERTY_STRING_TYPE = "property.string";

    protected abstract SchemaElementDefinition.BaseBuilder<T, ?> createBuilder();

    protected abstract SchemaElementDefinition.BaseBuilder<T, ?> createEmptyBuilder();

    @Test
    public void shouldNotBeAbleToAddPropertiesOnceBuilt() {
        // Given
        final T elementDef = createBuilder().build();

        // When / Then
        try {
            elementDef.getPropertyMap()
                    .put("new property", "string");
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldNotBeAbleToAddIdentifiersOnceBuilt() {
        // Given
        final T elementDef = createBuilder().build();

        // When / Then
        try {
            elementDef.getIdentifierMap()
                    .put(IdentifierType.SOURCE, "string");
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldNotBeAbleToModifyGroupByOnceBuilt() {
        // Given
        final T elementDef = createBuilder()
                .property("property", PROPERTY_STRING_TYPE)
                .build();

        // When / Then
        try {
            elementDef.getGroupBy().add("property");
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldNotBeAbleToModifyParentsOnceBuilt() {
        // Given
        final T elementDef = createBuilder()
                .property("property", PROPERTY_STRING_TYPE)
                .parents("parentGroup1")
                .build();

        // When / Then
        try {
            elementDef.getParents().add("parentGroup2");
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldReturnValidatorWithNoFunctionsWhenNoVerticesOrProperties() {
        // Given
        final T elementDef = createEmptyBuilder().build();

        // When
        final ElementFilter validator = elementDef.getValidator();

        // Then
        assertNull(validator.getFunctions());
    }

    @Test
    public void shouldReturnFullValidator() {
        // Given
        final T elementDef = createBuilder()
                .property("property", PROPERTY_STRING_TYPE)
                .build();

        setupSchema(elementDef);

        // When
        final ElementFilter validator = elementDef.getValidator();

        // Then
        int numFunctions = 2;
        if (elementDef instanceof SchemaEdgeDefinition) {
            numFunctions = 4;
        }
        assertEquals(numFunctions, validator.getFunctions().size());
        if (elementDef instanceof SchemaEdgeDefinition) {
            assertEquals(Integer.class.getName(), ((IsA) validator.getFunctions().get(0).getFunction()).getType());
            assertEquals(Collections.singletonList(IdentifierType.SOURCE.name()),
                    validator.getFunctions().get(0).getSelection());

            assertEquals(Date.class.getName(), ((IsA) validator.getFunctions().get(1).getFunction()).getType());
            assertEquals(Collections.singletonList(IdentifierType.DESTINATION.name()),
                    validator.getFunctions().get(1).getSelection());

            assertEquals(Boolean.class.getName(), ((IsA) validator.getFunctions().get(2).getFunction()).getType());
            assertEquals(Collections.singletonList(IdentifierType.DIRECTED.name()),
                    validator.getFunctions().get(2).getSelection());
        } else {
            assertEquals(Collections.singletonList(IdentifierType.VERTEX.name()),
                    validator.getFunctions().get(0).getSelection());
        }
        assertEquals(String.class.getName(), ((IsA) validator.getFunctions().get(numFunctions - 1).getFunction()).getType());
        assertEquals(Collections.singletonList("property"),
                validator.getFunctions().get(numFunctions - 1).getSelection());
    }


    @Test
    public void shouldBuildElementDefinition() {
        // Given
        final ElementFilter validator = mock(ElementFilter.class);
        final ElementFilter clonedValidator = mock(ElementFilter.class);

        given(validator.clone()).willReturn(clonedValidator);

        // When
        final T elementDef = createBuilder()
                .property(TestPropertyNames.PROP_1, "property.integer")
                .property(TestPropertyNames.PROP_2, "property.object")
                .validator(validator)
                .build();
        setupSchema(elementDef);

        // Then
        assertEquals(2, elementDef.getProperties().size());
        assertEquals(Integer.class, elementDef.getPropertyClass(TestPropertyNames.PROP_1));
        assertEquals(Object.class, elementDef.getPropertyClass(TestPropertyNames.PROP_2));
        assertSame(clonedValidator, elementDef.getValidator());
    }

    @Test
    public void shouldReturnFullAggregator() {
        // Given
        final T elementDef = createBuilder()
                .property("property", PROPERTY_STRING_TYPE)
                .build();

        setupSchema(elementDef);

        // When
        final ElementAggregator aggregator = elementDef.getAggregator();

        // Then
        assertEquals(1, aggregator.getFunctions().size());
        assertTrue(aggregator.getFunctions().get(0).getFunction() instanceof ExampleAggregateFunction);
        assertEquals(Collections.singletonList("property"),
                aggregator.getFunctions().get(0).getSelection());
    }

    @Test
    public void shouldMergeDifferentSchemaElementDefinitions() {
        // Given
        // When
        final T elementDef1 = createBuilder()
                .property(TestPropertyNames.PROP_1, "property.integer")
                .validator(new ElementFilter.Builder()
                        .select(TestPropertyNames.PROP_1)
                        .execute(new ExampleFilterFunction())
                        .build())
                .build();

        final T elementDef2 = createBuilder()
                .property(TestPropertyNames.PROP_2, "property.object")
                .validator(new ElementFilter.Builder()
                        .select(TestPropertyNames.PROP_2)
                        .execute(new ExampleFilterFunction())
                        .build())
                .groupBy(TestPropertyNames.PROP_2)
                .build();

        // When
        final T mergedDef = createEmptyBuilder()
                .merge(elementDef1)
                .merge(elementDef2)
                .build();

        // Then
        assertEquals(2, mergedDef.getProperties().size());
        assertNotNull(mergedDef.getPropertyTypeDef(TestPropertyNames.PROP_1));
        assertNotNull(mergedDef.getPropertyTypeDef(TestPropertyNames.PROP_2));
        assertEquals(Sets.newLinkedHashSet(Collections.singletonList(TestPropertyNames.PROP_2)),
                mergedDef.getGroupBy());
    }

    @Test
    public void shouldThrowExceptionWhenMergeSchemaElementDefinitionWithConflictingProperty() {
        // Given
        // When
        final T elementDef1 = createBuilder()
                .property(TestPropertyNames.PROP_1, "string")
                .build();

        final T elementDef2 = createBuilder()
                .property(TestPropertyNames.PROP_1, "int")
                .build();

        // When / Then
        try {
            createEmptyBuilder()
                    .merge(elementDef1)
                    .merge(elementDef2)
                    .build();
            fail("Exception expected");
        } catch (final SchemaException e) {
            assertTrue(e.getMessage().contains("property"));
        }
    }

    protected void setupSchema(final T elementDef) {
        final Schema.Builder schemaBuilder = new Schema.Builder()
                .type("id.integer", Integer.class)
                .type("id.date", Date.class)
                .type("directed.true", Boolean.class)
                .type("property.integer", Integer.class)
                .type("property.object", Object.class)
                .type(PROPERTY_STRING_TYPE, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(new ExampleAggregateFunction())
                        .build());
        if (elementDef instanceof SchemaEdgeDefinition) {
            schemaBuilder.edge(TestGroups.EDGE, ((SchemaEdgeDefinition) elementDef));
        } else {
            schemaBuilder.entity(TestGroups.ENTITY, ((SchemaEntityDefinition) elementDef));
        }
        schemaBuilder.build();
    }
}