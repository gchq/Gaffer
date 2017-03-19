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
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition.Builder;

import static org.junit.Assert.assertEquals;

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
        assertEquals(3, elementDef.getIdentifiers().size());
        assertEquals("id.integer", elementDef.getSource());
        assertEquals("id.date", elementDef.getDestination());
        assertEquals("directed.true", elementDef.getDirected());
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
        assertEquals("source.string", mergedDef.getSource());
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
        assertEquals("destination.string", mergedDef.getDestination());
    }
}