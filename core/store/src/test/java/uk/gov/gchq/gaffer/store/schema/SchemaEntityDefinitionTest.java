/*
 * Copyright 2016-2023 Crown Copyright
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

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.koryphe.ValidationResult;

import static org.assertj.core.api.Assertions.assertThat;

public class SchemaEntityDefinitionTest extends SchemaElementDefinitionTest<SchemaEntityDefinition> {

    @Override
    protected SchemaEntityDefinition.Builder createBuilder() {
        return new SchemaEntityDefinition.Builder()
                .vertex("id.integer");
    }

    @Override
    protected SchemaEntityDefinition.Builder createEmptyBuilder() {
        return new SchemaEntityDefinition.Builder();
    }

    @Test
    public void shouldBuildEntityDefinition() {
        // When
        final SchemaEntityDefinition elementDef = createBuilder().build();
        setupSchema(elementDef);

        // Then
        assertThat(elementDef.getIdentifiers()).hasSize(1);
        assertThat(elementDef.getVertex()).isEqualTo("id.integer");
    }

    @Test
    public void shouldOverrideVertexWhenMerging() {
        // Given
        final SchemaEntityDefinition elementDef1 = new SchemaEntityDefinition.Builder()
                .vertex("vertex.integer")
                .build();

        final SchemaEntityDefinition elementDef2 = new SchemaEntityDefinition.Builder()
                .vertex("vertex.string")
                .build();

        // When
        final SchemaEntityDefinition mergedDef = new SchemaEntityDefinition.Builder()
                .merge(elementDef1)
                .merge(elementDef2)
                .build();

        // Then
        assertThat(mergedDef.getVertex()).isEqualTo("vertex.string");
    }

    @Test
    public void shouldPassValidationWhenVertexDefined() {
        // Given
        final SchemaEntityDefinition elementDef = new SchemaEntityDefinition.Builder()
                .vertex("vertex.string")
                .build();

        new Schema.Builder()
                .entity(TestGroups.ENTITY, elementDef)
                .type("vertex.string", String.class)
                .build();

        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

        // When
        final ValidationResult result = validator.validate(elementDef);

        // Then
        assertThat(result.isValid()).isTrue();
    }

    @Test
    public void shouldFailValidationWhenVertexNotDefined() {
        // Given
        final SchemaEntityDefinition elementDef = new SchemaEntityDefinition.Builder()
                .vertex(null)
                .build();

        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, elementDef)
                .type("vertex.string", String.class)
                .build();

        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

        // When
        final ValidationResult result = validator.validate(elementDef);

        // Then
        assertThat(result.isValid()).isFalse();
    }

    @Test
    public void shouldReturnIdentifiersOrdered() {
        // Given
        final SchemaEntityDefinition elementDef = createBuilder()
                .identifier(IdentifierType.GROUP, PROPERTY_STRING_TYPE)
                .build();

        // When / Then
        assertThat(elementDef.getIdentifiers()).containsExactly(IdentifierType.VERTEX, IdentifierType.GROUP);
    }
}
