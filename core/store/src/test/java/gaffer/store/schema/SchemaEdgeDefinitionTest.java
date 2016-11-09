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

package gaffer.store.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import com.google.common.collect.Sets;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementAggregator;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.exception.SchemaException;
import gaffer.function.ExampleAggregateFunction;
import gaffer.function.ExampleFilterFunction;
import gaffer.function.IsA;
import gaffer.store.schema.SchemaEdgeDefinition.Builder;
import org.junit.Test;
import java.util.Collections;
import java.util.Date;

public class SchemaEdgeDefinitionTest {
    @Test
    public void shouldReturnValidatorWithNoFunctionsWhenNoProperties() {
        // Given
        final SchemaEdgeDefinition elementDef = new SchemaEdgeDefinition.Builder()
                .build();

        // When
        final ElementFilter validator = elementDef.getValidator();

        // Then
        assertNull(validator.getFunctions());
    }

    @Test
    public void shouldReturnFullValidator() {
        // Given
        final SchemaEdgeDefinition elementDef = new SchemaEdgeDefinition.Builder()
                .source("id.integer", Integer.class)
                .property("property", "property.string", String.class)
                .build();

        // When
        final ElementFilter validator = elementDef.getValidator();

        // Then
        assertEquals(2, validator.getFunctions().size());
        assertEquals(Integer.class.getName(), ((IsA) validator.getFunctions().get(0).getFunction()).getType());
        assertEquals(String.class.getName(), ((IsA) validator.getFunctions().get(1).getFunction()).getType());
        assertEquals(Collections.singletonList(IdentifierType.SOURCE.name()),
                validator.getFunctions().get(0).getSelection());
        assertEquals(Collections.singletonList("property"),
                validator.getFunctions().get(1).getSelection());
    }

    @Test
    public void shouldBuildElementDefinition() {
        // Given
        final ElementFilter validator = mock(ElementFilter.class);
        final ElementFilter clonedValidator = mock(ElementFilter.class);

        given(validator.clone()).willReturn(clonedValidator);

        // When
        final SchemaEdgeDefinition elementDef = new SchemaEdgeDefinition.Builder()
                .property(TestPropertyNames.PROP_1, "property.integer", Integer.class)
                .source("id.integer", Integer.class)
                .property(TestPropertyNames.PROP_2, "property.object", Object.class)
                .destination("id.date", Date.class)
                .directed("directed.true", Boolean.class)
                .validator(validator)
                .build();

        // Then
        assertEquals(2, elementDef.getProperties().size());
        assertEquals(Integer.class, elementDef.getPropertyClass(TestPropertyNames.PROP_1));
        assertEquals(Object.class, elementDef.getPropertyClass(TestPropertyNames.PROP_2));

        assertEquals(3, elementDef.getIdentifiers().size());
        assertEquals(Integer.class, elementDef.getIdentifierClass(IdentifierType.SOURCE));
        assertEquals(Date.class, elementDef.getIdentifierClass(IdentifierType.DESTINATION));
        assertEquals(Boolean.class, elementDef.getIdentifierClass(IdentifierType.DIRECTED));
        assertSame(clonedValidator, elementDef.getValidator());
    }

    @Test
    public void shouldReturnFullAggregator() {
        // Given
        final SchemaEdgeDefinition elementDef = new Builder()
                .source("id.integer", Integer.class)
                .property("property", "property.string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(new ExampleAggregateFunction())
                        .build())
                .build();

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
        final SchemaEdgeDefinition elementDef1 = new SchemaEdgeDefinition.Builder()
                .source("source.integer", Integer.class)
                .directed("directed.true", Boolean.class)
                .property(TestPropertyNames.PROP_1, "property.integer", Integer.class)
                .validator(new ElementFilter.Builder()
                        .select(TestPropertyNames.PROP_1)
                        .execute(new ExampleFilterFunction())
                        .build())
                .build();

        final SchemaEdgeDefinition elementDef2 = new SchemaEdgeDefinition.Builder()
                .destination("dest.integer", Integer.class)
                .property(TestPropertyNames.PROP_2, "property.object", Object.class)
                .validator(new ElementFilter.Builder()
                        .select(TestPropertyNames.PROP_2)
                        .execute(new ExampleFilterFunction())
                        .build())
                .groupBy(TestPropertyNames.PROP_2)
                .build();

        // When
        elementDef1.merge(elementDef2);

        // Then
        assertEquals("source.integer", elementDef1.getSource());
        assertEquals("dest.integer", elementDef1.getDestination());
        assertEquals("directed.true", elementDef1.getDirected());
        assertEquals(2, elementDef1.getProperties().size());
        assertNotNull(elementDef1.getPropertyTypeDef(TestPropertyNames.PROP_1));
        assertNotNull(elementDef1.getPropertyTypeDef(TestPropertyNames.PROP_2));
        assertEquals(Sets.newLinkedHashSet(Collections.singletonList(TestPropertyNames.PROP_2)),
                elementDef1.getGroupBy());
    }

    @Test
    public void shouldBeAbleToMergeSchemaElementDefinitionsWithItselfAndNotDuplicateObjects() {
        // Given
        // When
        final SchemaEdgeDefinition elementDef1 = new SchemaEdgeDefinition.Builder()
                .source("source.integer", Integer.class)
                .directed("directed.true", Boolean.class)
                .property(TestPropertyNames.PROP_1, "property.integer", Integer.class)
                .validator(new ElementFilter.Builder()
                        .select(TestPropertyNames.PROP_1)
                        .execute(new ExampleFilterFunction())
                        .build())
                .groupBy(TestPropertyNames.PROP_1)
                .build();

        // When
        elementDef1.merge(elementDef1);

        // Then
        assertEquals("source.integer", elementDef1.getSource());
        assertEquals("directed.true", elementDef1.getDirected());
        assertEquals(1, elementDef1.getProperties().size());
        assertNotNull(elementDef1.getPropertyTypeDef(TestPropertyNames.PROP_1));
        assertEquals(Sets.newLinkedHashSet(Collections.singletonList(TestPropertyNames.PROP_1)),
                elementDef1.getGroupBy());
    }

    @Test
    public void shouldThrowExceptionWhenMergeSchemaElementDefinitionWithConflictingSource() {
        // Given
        // When
        final SchemaEdgeDefinition elementDef1 = new SchemaEdgeDefinition.Builder()
                .source("source.integer", Integer.class)
                .build();

        final SchemaEdgeDefinition elementDef2 = new SchemaEdgeDefinition.Builder()
                .source("source.string", String.class)
                .build();

        // When / Then
        try {
            elementDef1.merge(elementDef2);
            fail("Exception expected");
        } catch (final SchemaException e) {
            assertTrue(e.getMessage().contains("identifier"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenMergeSchemaElementDefinitionWithConflictingDestination() {
        // Given
        // When
        final SchemaEdgeDefinition elementDef1 = new SchemaEdgeDefinition.Builder()
                .destination("destination.integer", Integer.class)
                .build();

        final SchemaEdgeDefinition elementDef2 = new SchemaEdgeDefinition.Builder()
                .destination("destination.string", String.class)
                .build();

        // When / Then
        try {
            elementDef1.merge(elementDef2);
            fail("Exception expected");
        } catch (final SchemaException e) {
            assertTrue(e.getMessage().contains("identifier"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenMergeSchemaElementDefinitionWithConflictingProperty() {
        // Given
        // When
        final SchemaEdgeDefinition elementDef1 = new SchemaEdgeDefinition.Builder()
                .property(TestPropertyNames.PROP_1, Integer.class)
                .build();

        final SchemaEdgeDefinition elementDef2 = new SchemaEdgeDefinition.Builder()
                .property(TestPropertyNames.PROP_1, String.class)
                .build();

        // When / Then
        try {
            elementDef1.merge(elementDef2);
            fail("Exception expected");
        } catch (final SchemaException e) {
            assertTrue(e.getMessage().contains("property"));
        }
    }
}