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
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition.Builder;
import uk.gov.gchq.koryphe.ValidationResult;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;

public class SchemaEdgeDefinitionTest extends SchemaElementDefinitionTest<SchemaEdgeDefinition> {

    @Override
    protected SchemaEdgeDefinition.Builder createBuilder() {
        return new SchemaEdgeDefinition.Builder()
                .source("id.integer")
                .destination("id.date")
                .directed("directed.true");
    }

    @Override
    protected SchemaEdgeDefinition.Builder createEmptyBuilder() {
        return new SchemaEdgeDefinition.Builder();
    }

    @Test
    public void shouldBuildEdgeDefinition() {
        // When
        final SchemaEdgeDefinition elementDef = createBuilder().build();
        setupSchema(elementDef);

        // Then
        assertThat(elementDef.getIdentifiers()).hasSize(3);
        assertThat(elementDef.getSource()).isEqualTo("id.integer");
        assertThat(elementDef.getDestination()).isEqualTo("id.date");
        assertThat(elementDef.getDirected()).isEqualTo("directed.true");
    }

    @Test
    public void shouldOverrideSourceWhenMerging() {
        // Given
        final SchemaEdgeDefinition elementDef1 = new SchemaEdgeDefinition.Builder()
                .source("source.integer")
                .build();

        final SchemaEdgeDefinition elementDef2 = new SchemaEdgeDefinition.Builder()
                .source("source.string")
                .build();

        // When
        final SchemaEdgeDefinition mergedDef = new Builder()
                .merge(elementDef1)
                .merge(elementDef2)
                .build();

        // Then
        assertThat(mergedDef.getSource()).isEqualTo("source.string");
    }

    @Test
    public void shouldOverrideDestinationWhenMerging() {
        // Given
        final SchemaEdgeDefinition elementDef1 = new SchemaEdgeDefinition.Builder()
                .destination("destination.integer")
                .build();

        final SchemaEdgeDefinition elementDef2 = new SchemaEdgeDefinition.Builder()
                .destination("destination.string")
                .build();

        // When
        final SchemaEdgeDefinition mergedDef = new Builder()
                .merge(elementDef1)
                .merge(elementDef2)
                .build();

        // Then
        assertThat(mergedDef.getDestination()).isEqualTo("destination.string");
    }

    @Test
    public void shouldPassValidationWhenEdgeSourceAndDestinationDefined() {
        // Given
        final SchemaEdgeDefinition elementDef = new SchemaEdgeDefinition.Builder()
                .source("src")
                .destination("dest")
                .directed(DIRECTED_EITHER)
                .build();

        final Schema schema = new Schema.Builder()
                .edge(TestGroups.EDGE, elementDef)
                .type("src", String.class)
                .type("dest", String.class)
                .type(DIRECTED_EITHER, Boolean.class)
                .build();

        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();

        // When
        final ValidationResult result = validator.validate(elementDef);

        // Then
        assertThat(result.isValid()).isTrue();
    }

    @Test
    public void shouldFailValidationWhenEdgeSourceOrDestinationNull() {
        // Given
        final SchemaEdgeDefinition elementDef = new SchemaEdgeDefinition.Builder()
                .source(null)
                .destination("dest")
                .build();

        final Schema schema = new Schema.Builder()
                .edge(TestGroups.EDGE, elementDef)
                .type("src", String.class)
                .type("dest", String.class)
                .build();

        final SchemaElementDefinitionValidator validator =
                new SchemaElementDefinitionValidator();

        // When
        final ValidationResult result = validator.validate(elementDef);

        // Then
        assertThat(result.isValid()).isFalse();
    }

    @Test
    public void shouldReturnIdentifiersOrdered() {
        // Given
        final SchemaEdgeDefinition elementDef = createBuilder()
                .identifier(IdentifierType.MATCHED_VERTEX, PROPERTY_STRING_TYPE)
                .identifier(IdentifierType.GROUP, PROPERTY_STRING_TYPE)
                .build();

        // When / Then
        assertThat(elementDef.getIdentifiers()).containsExactly(
                IdentifierType.SOURCE, IdentifierType.DESTINATION, IdentifierType.DIRECTED, IdentifierType.MATCHED_VERTEX, IdentifierType.GROUP);
    }

}
