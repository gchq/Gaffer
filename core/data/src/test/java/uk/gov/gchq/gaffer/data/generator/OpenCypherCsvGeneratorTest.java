/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.data.generator;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;

import java.util.LinkedHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static uk.gov.gchq.gaffer.data.generator.OpenCypherCsvElementGenerator.DESTINATION;
import static uk.gov.gchq.gaffer.data.generator.OpenCypherCsvElementGenerator.EDGE_GROUP;
import static uk.gov.gchq.gaffer.data.generator.OpenCypherCsvElementGenerator.ENTITY_GROUP;
import static uk.gov.gchq.gaffer.data.generator.OpenCypherCsvElementGenerator.SOURCE;
import static uk.gov.gchq.gaffer.data.generator.OpenCypherCsvElementGenerator.VERTEX;

class OpenCypherCsvGeneratorTest {

    @Test
    public void builderShouldCreatePopulatedGenerator() {
        // Given
        final LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put(String.valueOf(IdentifierType.VERTEX), VERTEX);
        fields.put("ENTITY_GROUP", ENTITY_GROUP);
        fields.put("EDGE_GROUP", EDGE_GROUP);
        fields.put(String.valueOf(IdentifierType.SOURCE), SOURCE);
        fields.put(String.valueOf(IdentifierType.DESTINATION), DESTINATION);
        final OpenCypherCsvGenerator openCypherCsvGenerator = new OpenCypherCsvGenerator.Builder()
                .headers(fields)
                .neo4jFormat(false)
                .build();

        // Then
        assertThat(openCypherCsvGenerator.getFields())
                .hasSize(5);
        assertFalse(openCypherCsvGenerator.isNeo4jFormat());
    }

    @Test
    public void shouldSetHeadersToMatchNeptunesFormat() {
        // Given
        final LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("Property1", "count:int");
        final OpenCypherCsvGenerator openCypherCsvGenerator = new OpenCypherCsvGenerator.Builder()
                .headers(fields)
                .neo4jFormat(false)
                .build();
        openCypherCsvGenerator.setFields(fields);
        // Then
        assertThat(openCypherCsvGenerator.getHeader().contains(":ID,:LABEL,:TYPE,:START_ID,:END_ID,count:int"));

    }

    @Test
    public void shouldSetHeadersToMatchNeo4jFormat() {
        // Given
        final LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("Property1", "count:int");
        final OpenCypherCsvGenerator openCypherCsvGenerator = new OpenCypherCsvGenerator.Builder()
                .headers(fields)
                .neo4jFormat(true)
                .build();
        openCypherCsvGenerator.setFields(fields);
        // Then
        assertThat(openCypherCsvGenerator.getHeader().contains("_id,_labels,_type,_start,_end,count:int"));

    }

    @Test
    public void shouldReturnEmptyStringIfBuilderNotUsed() {
        // Given
        Entity entity = new Entity.Builder()
                .group("Foo")
                .vertex("vertex")
                .property("propertyName", "propertyValue")
                .build();
        final OpenCypherCsvGenerator openCypherCsvGenerator = new OpenCypherCsvGenerator();
        // Then
        assertThat(openCypherCsvGenerator._apply(entity).matches(""));

    }
}
