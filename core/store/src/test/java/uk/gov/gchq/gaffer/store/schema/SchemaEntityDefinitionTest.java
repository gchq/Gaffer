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

import static org.junit.Assert.assertEquals;

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
        assertEquals(1, elementDef.getIdentifiers().size());
        assertEquals("id.integer", elementDef.getVertex());
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
        assertEquals("vertex.string", mergedDef.getVertex());
    }
}