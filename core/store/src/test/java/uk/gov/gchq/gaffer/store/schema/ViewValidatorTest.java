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
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.ExampleFilterFunction;
import uk.gov.gchq.gaffer.function.ExampleTransformFunction;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ViewValidatorTest {
    @Test
    public void shouldValidateAndReturnTrueWhenEmptyFunctions() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder().build();
        final Schema schema = new Schema();

        // When
        final boolean isValid = validator.validate(view, schema, false);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateAndReturnFalseWhenEntityTransientPropertyIsInSchema() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .transientProperty(TestPropertyNames.PROP_1, String.class)
                        .build())
                .build();
        final Schema schema = new Schema.Builder()
                .type("prop1", Object.class)
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, "prop1")
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema, false);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldValidateAndReturnTrueWhenEntityTransientPropertyIsNotInSchema() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .transientProperty(TestPropertyNames.PROP_1, String.class)
                        .build())
                .build();
        final Schema schema = new Schema.Builder()
                .type("obj", Object.class)
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_2, "obj")
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema, false);

        // Then
        assertTrue(isValid);
    }


    @Test
    public void shouldValidateAndReturnFalseWhenEntityFilterSelectionMissingProperty() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .transientProperty(TestPropertyNames.PROP_3, String.class)
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .build();
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema, false);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldValidateAndReturnFalseWhenEntityTransformerSelectionMissingProperty() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .transientProperty(TestPropertyNames.PROP_3, String.class)
                        .transformer(new ElementTransformer.Builder()
                                .select(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                                .project(TestPropertyNames.PROP_3)
                                .execute(new ExampleTransformFunction())
                                .build())
                        .build())
                .build();
        final Schema schema = new Schema.Builder()
                .type("obj", Object.class)
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, "obj")
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema, false);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldValidateAndReturnFalseWhenEntityTransformerProjectsToMissingProperty() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .transformer(new ElementTransformer.Builder()
                                .select(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                                .project(TestPropertyNames.PROP_3)
                                .execute(new ExampleTransformFunction())
                                .build())
                        .build())
                .build();
        final Schema schema = new Schema.Builder()
                .type("string", Object.class)
                .type("int", Object.class)
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, "string")
                        .property(TestPropertyNames.PROP_2, "int")
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema, false);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldValidateAndReturnTrueWhenEntityTransformerIsValid() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .transformer(new ElementTransformer.Builder()
                                .select(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                                .project(TestPropertyNames.PROP_3)
                                .execute(new ExampleTransformFunction())
                                .build())
                        .build())
                .build();
        final Schema schema = new Schema.Builder()
                .type("double", Double.class)
                .type("int", Integer.class)
                .type("string", String.class)
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, "double")
                        .property(TestPropertyNames.PROP_2, "int")
                        .property(TestPropertyNames.PROP_3, "string")
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema, false);

        // Then
        assertTrue(isValid);
    }


    @Test
    public void shouldValidateAndReturnFalseWhenEdgeTransientPropertyIsInSchema() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .transientProperty(TestPropertyNames.PROP_1, String.class)
                        .build())
                .build();
        final Schema schema = new Schema.Builder()
                .type("obj", Object.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, "obj")
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema, false);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldValidateAndReturnTrueWhenEdgeTransientPropertyIsNotInSchema() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .transientProperty(TestPropertyNames.PROP_1, String.class)
                        .build())
                .build();
        final Schema schema = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_2, "obj")
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema, false);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateAndReturnFalseWhenEdgeFilterSelectionMissingProperty() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .transientProperty(TestPropertyNames.PROP_3, String.class)
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .build();
        final Schema schema = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema, false);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldValidateAndReturnFalseWhenEdgeTransformerSelectionMissingProperty() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .transientProperty(TestPropertyNames.PROP_3, String.class)
                        .transformer(new ElementTransformer.Builder()
                                .select(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                                .project(TestPropertyNames.PROP_3)
                                .execute(new ExampleTransformFunction())
                                .build())
                        .build())
                .build();
        final Schema schema = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, "obj")
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema, false);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldValidateAndReturnFalseWhenEdgeTransformerProjectsToMissingProperty() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .transformer(new ElementTransformer.Builder()
                                .select(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                                .project(TestPropertyNames.PROP_3)
                                .execute(new ExampleTransformFunction())
                                .build())
                        .build())
                .build();
        final Schema schema = new Schema.Builder()
                .type("int", Integer.class)
                .type("string", String.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, "int")
                        .property(TestPropertyNames.PROP_2, "string")
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema, false);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldValidateAndReturnTrueWhenEdgeTransformerIsValid() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder()
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .transformer(new ElementTransformer.Builder()
                                .select(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                                .project(TestPropertyNames.PROP_3)
                                .execute(new ExampleTransformFunction())
                                .build())
                        .build())
                .build();
        final Schema schema = new Schema.Builder()
                .type("double", Double.class)
                .type("int", Integer.class)
                .type("string", String.class)
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, "double")
                        .property(TestPropertyNames.PROP_2, "int")
                        .property(TestPropertyNames.PROP_3, "string")
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema, false);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateAndReturnTrueNoGroupByProperties() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY)
                .edge(TestGroups.EDGE)
                .build();
        final Schema schema = new Schema.Builder()
                .type("vertex", String.class)
                .type("true", Boolean.class)
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("vertex")
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("vertex")
                        .destination("vertex")
                        .directed("true")
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema, false);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateAndReturnTrueForNullView() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder().build();
        final Schema schema = new Schema.Builder()
                .type("vertex", String.class)
                .type("true", Boolean.class)
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("vertex")
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("vertex")
                        .destination("vertex")
                        .directed("true")
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema, false);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateAndReturnTrueWhenGroupByPropertiesInSchema() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy(TestPropertyNames.PROP_1)
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .build())
                .build();
        final Schema schema = new Schema.Builder()
                .type("vertex", String.class)
                .type("string|ColumnQualifier", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("string|Value", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("true", Boolean.class)
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("vertex")
                        .property(TestPropertyNames.PROP_1, "string|ColumnQualifier")
                        .property(TestPropertyNames.PROP_2, "string|ColumnQualifier")
                        .property(TestPropertyNames.PROP_3, "string|Value")
                        .groupBy(TestPropertyNames.PROP_1)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("vertex")
                        .destination("vertex")
                        .directed("true")
                        .property(TestPropertyNames.PROP_1, "string|ColumnQualifier")
                        .property(TestPropertyNames.PROP_2, "string|ColumnQualifier")
                        .property(TestPropertyNames.PROP_3, "string|Value")
                        .groupBy(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .build())
                .visibilityProperty(TestPropertyNames.PROP_2)
                .build();

        // When
        final boolean isValid = validator.validate(view, schema, false);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateAndReturnFalseWhenTimestampGroupByPropertyUsedInEntity() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .build())
                .build();
        final Schema schema = new Schema.Builder()
                .type("vertex", String.class)
                .type("string|Timestamp", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("string|Value", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("true", Boolean.class)
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("vertex")
                        .property(TestPropertyNames.PROP_1, "string|Timestamp")
                        .property(TestPropertyNames.PROP_2, "string|Value")
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("vertex")
                        .destination("vertex")
                        .directed("true")
                        .property(TestPropertyNames.PROP_2, "string|Value")
                        .build())
                .timestampProperty(TestPropertyNames.PROP_1)
                .build();

        // When
        final boolean isValid = validator.validate(view, schema, false);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldValidateAndReturnFalseWhenGroupByPropertyNotInSchema() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy(TestPropertyNames.PROP_1)
                        .build())
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy(TestPropertyNames.PROP_2)
                        .build())
                .build();
        final Schema schema = new Schema.Builder()
                .type("vertex", String.class)
                .type("string|Timestamp", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("string|Value", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type("true", Boolean.class)
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("vertex")
                        .property(TestPropertyNames.PROP_2, "string|Value")
                        .groupBy(TestPropertyNames.PROP_2)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("vertex")
                        .destination("vertex")
                        .directed("true")
                        .property(TestPropertyNames.PROP_1, "string|Timestamp")
                        .property(TestPropertyNames.PROP_2, "string|Value")
                        .groupBy(TestPropertyNames.PROP_1)
                        .build())
                .timestampProperty(TestPropertyNames.PROP_1)
                .build();

        // When
        final boolean isValid = validator.validate(view, schema, true);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldValidateAndReturnTrueWhenPostTransformerFilterSet() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .transientProperty(TestPropertyNames.PROP_3, String.class)
                        .transformer(new ElementTransformer.Builder()
                                .select(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                                .project(TestPropertyNames.PROP_3)
                                .execute(new ExampleTransformFunction())
                                .build())
                        .postTransformFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_3)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .build();
        final Schema schema = new Schema.Builder()
                .type("obj", Object.class)
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, "obj")
                        .property(TestPropertyNames.PROP_2, "obj")
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema, false);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateAndReturnFalseWhenPostTransformerSelectionDoesNotExist() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .transientProperty(TestPropertyNames.PROP_3, String.class)
                        .transformer(new ElementTransformer.Builder()
                                .select(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                                .project(TestPropertyNames.PROP_3)
                                .execute(new ExampleTransformFunction())
                                .build())
                        .postTransformFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.TRANSIENT_1)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .build();
        final Schema schema = new Schema.Builder()
                .type("obj", Object.class)
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, "obj")
                        .property(TestPropertyNames.PROP_2, "obj")
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema, false);
        // Then
        assertFalse(isValid);
    }

}