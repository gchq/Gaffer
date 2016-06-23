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

package gaffer.accumulostore.schema;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import gaffer.accumulostore.utils.StorePositions;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.elementdefinition.view.View;
import gaffer.store.schema.Schema;
import gaffer.store.schema.SchemaEdgeDefinition;
import gaffer.store.schema.SchemaEntityDefinition;
import gaffer.store.schema.TypeDefinition;
import org.junit.Test;

public class AccumuloViewValidatorTest {
    @Test
    public void shouldValidateAndReturnTrueNoGroupByPropertyPositions() {
        // Given
        final AccumuloViewValidator validator = new AccumuloViewValidator();
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY)
                .edge(TestGroups.EDGE)
                .build();
        final Schema schema = new Schema.Builder()
                .type("vertex", String.class)
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("vertex")
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("vertex")
                        .destination("vertex")
                        .directed(Boolean.class)
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateAndReturnTrueForNullView() {
        // Given
        final AccumuloViewValidator validator = new AccumuloViewValidator();
        final View view = new View();
        final Schema schema = new Schema.Builder()
                .type("vertex", String.class)
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("vertex")
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("vertex")
                        .destination("vertex")
                        .directed(Boolean.class)
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateAndReturnTrueWhenOnlyColumnQualifierAndVisibilityGroupByPropertyPositions() {
        // Given
        final AccumuloViewValidator validator = new AccumuloViewValidator();
        final View view = new View.Builder()
                .groupByProperties(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                .entity(TestGroups.ENTITY)
                .edge(TestGroups.EDGE)
                .build();
        final Schema schema = new Schema.Builder()
                .type("vertex", String.class)
                .type("string|ColumnQualifier", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .position(StorePositions.COLUMN_QUALIFIER.name())
                        .build())
                .type("string|Visibility", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .position(StorePositions.VISIBILITY.name())
                        .build())
                .type("string|Value", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .position(StorePositions.VALUE.name())
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("vertex")
                        .property(TestPropertyNames.PROP_1, "string|ColumnQualifier")
                        .property(TestPropertyNames.PROP_2, "string|Visibility")
                        .property(TestPropertyNames.PROP_3, "string|Value")
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("vertex")
                        .destination("vertex")
                        .directed(Boolean.class)
                        .property(TestPropertyNames.PROP_1, "string|ColumnQualifier")
                        .property(TestPropertyNames.PROP_2, "string|Visibility")
                        .property(TestPropertyNames.PROP_3, "string|Value")
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateAndReturnFalseWhenTimestampGroupByPropertyPositionUsedInEntity() {
        // Given
        final AccumuloViewValidator validator = new AccumuloViewValidator();
        final View view = new View.Builder()
                .groupByProperties(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                .entity(TestGroups.ENTITY)
                .edge(TestGroups.EDGE)
                .build();
        final Schema schema = new Schema.Builder()
                .type("vertex", String.class)
                .type("string|Timestamp", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .position(StorePositions.TIMESTAMP.name())
                        .build())
                .type("string|Value", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .position(StorePositions.VALUE.name())
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("vertex")
                        .property(TestPropertyNames.PROP_1, "string|Timestamp")
                        .property(TestPropertyNames.PROP_2, "string|Value")
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("vertex")
                        .destination("vertex")
                        .directed(Boolean.class)
                        .property(TestPropertyNames.PROP_2, "string|Value")
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldValidateAndReturnFalseWhenTimestampGroupByPropertyPositionUsedInEdge() {
        // Given
        final AccumuloViewValidator validator = new AccumuloViewValidator();
        final View view = new View.Builder()
                .groupByProperties(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                .entity(TestGroups.ENTITY)
                .edge(TestGroups.EDGE)
                .build();
        final Schema schema = new Schema.Builder()
                .type("vertex", String.class)
                .type("string|Timestamp", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .position(StorePositions.TIMESTAMP.name())
                        .build())
                .type("string|Value", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .position(StorePositions.VALUE.name())
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("vertex")
                        .property(TestPropertyNames.PROP_2, "string|Value")
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("vertex")
                        .destination("vertex")
                        .directed(Boolean.class)
                        .property(TestPropertyNames.PROP_1, "string|Timestamp")
                        .property(TestPropertyNames.PROP_2, "string|Value")
                        .build())
                .build();

        // When
        final boolean isValid = validator.validate(view, schema);

        // Then
        assertFalse(isValid);
    }
}