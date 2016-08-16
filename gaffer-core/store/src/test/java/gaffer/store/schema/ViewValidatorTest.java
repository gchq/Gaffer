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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.element.function.ElementTransformer;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.function.ExampleFilterFunction;
import gaffer.function.ExampleTransformFunction;
import org.junit.Test;

public class ViewValidatorTest {
    @Test
    public void shouldValidateAndReturnTrueWhenEmptyFunctions() {
        // Given
        final ViewValidator validator = new ViewValidator();
        final View view = new View();
        final Schema schema = new Schema();

        // When
        final boolean isValid = validator.validate(view, schema);

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
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, Object.class)
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema);

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
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_2, Object.class)
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema);

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
                        .filter(new ElementFilter.Builder()
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
        final boolean isValid = validator.validate(view, schema);

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
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, Object.class)
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema);

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
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, String.class)
                        .property(TestPropertyNames.PROP_2, Integer.class)
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema);

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
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, Double.class)
                        .property(TestPropertyNames.PROP_2, Integer.class)
                        .property(TestPropertyNames.PROP_3, String.class)
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema);

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
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, Object.class)
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema);

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
                        .property(TestPropertyNames.PROP_2, Object.class)
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema);

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
                        .filter(new ElementFilter.Builder()
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
        final boolean isValid = validator.validate(view, schema);

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
                        .property(TestPropertyNames.PROP_1, Object.class)
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema);

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
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, Integer.class)
                        .property(TestPropertyNames.PROP_2, String.class)
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema);

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
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, Double.class)
                        .property(TestPropertyNames.PROP_2, Integer.class)
                        .property(TestPropertyNames.PROP_3, String.class)
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema);

        // Then
        assertTrue(isValid);
    }
}