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
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.function.ExampleAggregateFunction;
import uk.gov.gchq.gaffer.function.ExampleFilterFunction;
import uk.gov.gchq.gaffer.function.IsA;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition.Builder;
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
                .source("id.integer")
                .property("property", "property.string")
                .build();

        final Schema schema = new Schema.Builder()
                .type("id.integer", Integer.class)
                .type("property.string", String.class)
                .edge("edge", elementDef)
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
                .property(TestPropertyNames.PROP_1, "property.integer")
                .source("id.integer")
                .property(TestPropertyNames.PROP_2, "property.object")
                .destination("id.date")
                .directed("directed.true")
                .validator(validator)
                .build();
        final Schema schema = new Schema.Builder()
                .type("id.integer", Integer.class)
                .type("id.date", Date.class)
                .type("directed.true", Boolean.class)
                .type("property.integer", Integer.class)
                .type("property.object", Object.class)
                .edge("edge", elementDef)
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
                .source("id.integer")
                .property("property", "property.string")
                .build();

        final Schema schema = new Schema.Builder()
                .type("property.string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(new ExampleAggregateFunction())
                        .build())
                .type("id.integer", Integer.class)
                .edge("edge", elementDef)
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
                .source("source.integer")
                .directed("directed.true")
                .property(TestPropertyNames.PROP_1, "property.integer")
                .validator(new ElementFilter.Builder()
                        .select(TestPropertyNames.PROP_1)
                        .execute(new ExampleFilterFunction())
                        .build())
                .build();

        final SchemaEdgeDefinition elementDef2 = new SchemaEdgeDefinition.Builder()
                .destination("dest.integer")
                .property(TestPropertyNames.PROP_2, "property.object")
                .validator(new ElementFilter.Builder()
                        .select(TestPropertyNames.PROP_2)
                        .execute(new ExampleFilterFunction())
                        .build())
                .groupBy(TestPropertyNames.PROP_2)
                .build();

        // When
        final SchemaEdgeDefinition mergedDef = new Builder()
                .merge(elementDef1)
                .merge(elementDef2)
                .build();

        // Then
        assertEquals("source.integer", mergedDef.getSource());
        assertEquals("dest.integer", mergedDef.getDestination());
        assertEquals("directed.true", mergedDef.getDirected());
        assertEquals(2, mergedDef.getProperties().size());
        assertNotNull(mergedDef.getPropertyTypeDef(TestPropertyNames.PROP_1));
        assertNotNull(mergedDef.getPropertyTypeDef(TestPropertyNames.PROP_2));
        assertEquals(Sets.newLinkedHashSet(Collections.singletonList(TestPropertyNames.PROP_2)),
                mergedDef.getGroupBy());
    }

    @Test
    public void shouldBeAbleToMergeSchemaElementDefinitionsWithItselfAndNotDuplicateObjects() {
        // Given
        // When
        final SchemaEdgeDefinition elementDef1 = new SchemaEdgeDefinition.Builder()
                .source("source.integer")
                .directed("directed.true")
                .property(TestPropertyNames.PROP_1, "property.integer")
                .validator(new ElementFilter.Builder()
                        .select(TestPropertyNames.PROP_1)
                        .execute(new ExampleFilterFunction())
                        .build())
                .groupBy(TestPropertyNames.PROP_1)
                .build();

        // When
        final SchemaEdgeDefinition mergedDef = new Builder()
                .merge(elementDef1)
                .merge(elementDef1)
                .build();

        // Then
        assertEquals("source.integer", mergedDef.getSource());
        assertEquals("directed.true", mergedDef.getDirected());
        assertEquals(1, mergedDef.getProperties().size());
        assertNotNull(mergedDef.getPropertyTypeDef(TestPropertyNames.PROP_1));
        assertEquals(Sets.newLinkedHashSet(Collections.singletonList(TestPropertyNames.PROP_1)),
                mergedDef.getGroupBy());
    }

    @Test
    public void shouldThrowExceptionWhenMergeSchemaElementDefinitionWithConflictingSource() {
        // Given
        // When
        final SchemaEdgeDefinition elementDef1 = new SchemaEdgeDefinition.Builder()
                .source("source.integer")
                .build();

        final SchemaEdgeDefinition elementDef2 = new SchemaEdgeDefinition.Builder()
                .source("source.string")
                .build();

        // When / Then
        try {
            new Builder()
                    .merge(elementDef1)
                    .merge(elementDef2)
                    .build();
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
                .destination("destination.integer")
                .build();

        final SchemaEdgeDefinition elementDef2 = new SchemaEdgeDefinition.Builder()
                .destination("destination.string")
                .build();

        // When / Then
        try {
            new Builder()
                    .merge(elementDef1)
                    .merge(elementDef2)
                    .build();
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
                .property(TestPropertyNames.PROP_1, "string")
                .build();

        final SchemaEdgeDefinition elementDef2 = new SchemaEdgeDefinition.Builder()
                .property(TestPropertyNames.PROP_1, "int")
                .build();

        // When / Then
        try {
            new Builder()
                    .merge(elementDef1)
                    .merge(elementDef2)
                    .build();
            fail("Exception expected");
        } catch (final SchemaException e) {
            assertTrue(e.getMessage().contains("property"));
        }
    }
}