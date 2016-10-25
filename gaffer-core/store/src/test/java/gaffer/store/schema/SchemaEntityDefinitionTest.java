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
import org.junit.Test;
import java.util.Collections;

public class SchemaEntityDefinitionTest {
    @Test
    public void shouldReturnValidatorWithNoFunctionsWhenNoProperties() {
        // Given
        final SchemaEntityDefinition elementDef = new SchemaEntityDefinition.Builder()
                .build();

        // When
        final ElementFilter validator = elementDef.getValidator();

        // Then
        assertNull(validator.getFunctions());
    }

    @Test
    public void shouldReturnFullValidator() {
        // Given
        final SchemaEntityDefinition elementDef = new SchemaEntityDefinition.Builder()
                .vertex("id.integer", Integer.class)
                .property("property", "property.string", String.class)
                .vertex(Integer.class)
                .property("property", String.class)
                .build();

        // When
        final ElementFilter validator = elementDef.getValidator();

        // Then
        assertEquals(2, validator.getFunctions().size());
        assertEquals(Integer.class.getName(), ((IsA) validator.getFunctions().get(0).getFunction()).getType());
        assertEquals(String.class.getName(), ((IsA) validator.getFunctions().get(1).getFunction()).getType());
        assertEquals(Collections.singletonList(IdentifierType.VERTEX.name()),
                validator.getFunctions().get(0).getSelection());
        assertEquals(Collections.singletonList("property"),
                validator.getFunctions().get(1).getSelection());
    }

    @Test
    public void shouldBuildEntityDefinition() {
        // Given
        final ElementFilter validator = mock(ElementFilter.class);
        final ElementFilter clonedValidator = mock(ElementFilter.class);
        given(validator.clone()).willReturn(clonedValidator);

        // When
        final SchemaEntityDefinition elementDef = new SchemaEntityDefinition.Builder()
                .property(TestPropertyNames.PROP_1, "property.string", String.class)
                .vertex("id.integer", Integer.class)
                .property(TestPropertyNames.PROP_2, "property.object", Object.class)
                .validator(validator)
                .build();

        // Then
        assertEquals(2, elementDef.getProperties().size());
        assertTrue(elementDef.containsProperty(TestPropertyNames.PROP_1));
        assertTrue(elementDef.containsProperty(TestPropertyNames.PROP_2));

        assertEquals(1, elementDef.getIdentifiers().size());
        assertEquals(Integer.class, elementDef.getIdentifierClass(IdentifierType.VERTEX));
        assertSame(clonedValidator, elementDef.getValidator());
    }

    @Test
    public void shouldReturnFullAggregator() {
        // Given
        final SchemaEntityDefinition elementDef = new SchemaEntityDefinition.Builder()
                .vertex("id.integer", Integer.class)
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
        final SchemaEntityDefinition elementDef1 = new SchemaEntityDefinition.Builder()
                .vertex("id.integer", Integer.class)
                .property(TestPropertyNames.PROP_1, "property.integer", Integer.class)
                .validator(new ElementFilter.Builder()
                        .select(TestPropertyNames.PROP_1)
                        .execute(new ExampleFilterFunction())
                        .build())
                .build();

        final SchemaEntityDefinition elementDef2 = new SchemaEntityDefinition.Builder()
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
        assertEquals("id.integer", elementDef1.getVertex());
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
        final SchemaEntityDefinition elementDef1 = new SchemaEntityDefinition.Builder()
                .vertex("id.integer", Integer.class)
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
        assertEquals("id.integer", elementDef1.getVertex());
        assertEquals(1, elementDef1.getProperties().size());
        assertNotNull(elementDef1.getPropertyTypeDef(TestPropertyNames.PROP_1));

        assertEquals(Sets.newLinkedHashSet(Collections.singletonList(TestPropertyNames.PROP_1)),
                elementDef1.getGroupBy());
    }

    @Test
    public void shouldThrowExceptionWhenMergeSchemaElementDefinitionWithConflictingDestination() {
        // Given
        // When
        final SchemaEntityDefinition elementDef1 = new SchemaEntityDefinition.Builder()
                .vertex("vertex.integer", Integer.class)
                .build();

        final SchemaEntityDefinition elementDef2 = new SchemaEntityDefinition.Builder()
                .vertex("vertex.string", String.class)
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
        final SchemaEntityDefinition elementDef1 = new SchemaEntityDefinition.Builder()
                .property(TestPropertyNames.PROP_1, Integer.class)
                .build();

        final SchemaEntityDefinition elementDef2 = new SchemaEntityDefinition.Builder()
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