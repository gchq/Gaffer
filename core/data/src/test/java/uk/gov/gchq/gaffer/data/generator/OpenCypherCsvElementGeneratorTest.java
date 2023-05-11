/*
 * Copyright 2022-2023 Crown Copyright
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

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.koryphe.util.DateUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public abstract class OpenCypherCsvElementGeneratorTest<T extends OpenCypherCsvElementGenerator> {
    protected abstract T getGenerator(final boolean trim, final char delimiter, final String nullString);

    protected abstract String getResourcePath();

    private T getGenerator() {
        return getGenerator(true, ',', "");
    }

    private Iterable<String> getInputData(String filename) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/openCypherCSVs/" + getResourcePath() + filename)))) {
            return reader.lines().collect(Collectors.toList());
        }
    }

    @Test
    private void shouldBuildGenerator() throws IOException {
        // Given
        final T generator = getGenerator();

        // When / Then
        assertThat(generator.getDelimiter()).isEqualTo(',');
        assertThat(generator.getNullString()).isEqualTo("");
        assertThat(generator.getTrim()).isEqualTo(true);
    }

    @Test
    private void shouldGenerateBasicEntity() throws IOException {
        // Given
        final Iterable<String> lines = getInputData("BasicEntities.csv");

        // When
        final T generator = getGenerator();
        final Iterable<Element> elements = (Iterable<Element>) generator.apply(lines);

        // Then
        assertThat(elements).containsExactly(
                new Entity("person", "v1"),
                new Entity("software", "v2")
        );
    }

    @Test
    private void shouldGenerateBasicEdge() throws IOException {
        // Given
        final Iterable<String> lines = getInputData("BasicEdge.csv");

        // When
        final T generator = getGenerator();
        Iterable<Edge> edge = (Iterable<Edge>) generator.apply(lines);

        // Then
        assertThat(edge).containsExactly(
                new Edge("created", "v1", "v2", true)
        );
    }

    @Test
    private void shouldGenerateBasicEntityFromPipeDelimitedCsv() throws IOException {
        // Given
        final Iterable<String> lines = getInputData("BasicEntityPipeDelimited.csv");

        // When
        final T generator = getGenerator(true, '|', "");
        final Iterable<Element> elements = (Iterable<Element>) generator.apply(lines);

        // Then
        assertThat(elements).containsExactly(
                new Entity("person", "v1"),
                new Entity("software", "v2")
        );
    }

    @Test
    private void shouldGenerateBasicEntityFromCsvWithWhiteSpacePadding() throws IOException {
        // Given
        final Iterable<String> lines = getInputData("BasicEntityPaddingSpaces.csv");

        // When
        final T generator = getGenerator();
        final Iterable<Element> elements = (Iterable<Element>) generator.apply(lines);

        // Then
        assertThat(elements).containsExactly(
                new Entity("person", "v1"),
                new Entity("software", "v2")
        );
    }

    @Test
    private void shouldGenerateEdgeWithID() throws IOException {
        // Given
        final Iterable<String> lines = getInputData("EdgeWithID.csv");

        // When
        final T generator = getGenerator();
        Iterable<Edge> edge = (Iterable<Edge>) generator.apply(lines);

        // Then
        assertThat(edge).containsExactly(
                new Edge.Builder()
                        .group("created")
                        .source("v1")
                        .dest("v2")
                        .directed(true)
                        .property("edge-id", "e1")
                        .build()
        );
    }

    @Test
    private void shouldGenerateBasicEntityFromCsvWithValuesSurroundedByDoubleQuotes() throws IOException {
        // Given
        final Iterable<String> lines = getInputData("BasicEntityQuotedValues.csv");

        // When
        final T generator = getGenerator();
        final Iterable<Element> elements = (Iterable<Element>) generator.apply(lines);

        // Then
        assertThat(elements).containsExactly(
                new Entity("person", "v1"),
                new Entity("software", "v2")
        );
    }

    @Test
    private void shouldGenerateBasicEntitiesAndEdgesCsv() throws IOException {
        // Given
        final Iterable<String> lines = getInputData("BasicEntitiesAndEdges.csv");

        // When
        final T generator = getGenerator();
        final Iterable<Element> elements = (Iterable<Element>) generator.apply(lines);

        // Then
        assertThat(elements).containsExactly(
                new Entity("person", "v1"),
                new Entity("software", "v2"),
                new Edge.Builder()
                        .group("created")
                        .source("v1")
                        .dest("v2")
                        .directed(true)
                        .property("edge-id", "e1")
                        .build()
        );
    }

    @Test
    private void shouldGenerateEntityWithPropertiesNoTypes() throws IOException {
        // Given
        final Iterable<String> lines = getInputData("EntityWithPropertiesNoTypes.csv");

        // When
        final T generator = getGenerator();
        final Iterable<Element> elements = (Iterable<Element>) generator.apply(lines);

        // Then
        assertThat(elements).containsExactly(
                new Entity.Builder()
                        .group("person")
                        .vertex("v1")
                        .property("name", "marko")
                        .property("age", "29")
                        .build(),


                new Entity.Builder()
                        .group("software")
                        .vertex("v2")
                        .property("name", "lop")
                        .property("lang", "java")
                        .build()
        );
    }


    @Test
    private void shouldGenerateEntityWithPropertiesWithCorrectTypes() throws IOException {
        // Given
        final Iterable<String> lines = getInputData("EntityWithPropertiesOfMultipleTypes.csv");

        // When
        final T generator = getGenerator();
        final Iterable<Element> elements = (Iterable<Element>) generator.apply(lines);

        // Then
        assertThat(elements).containsExactly(
                new Entity.Builder()
                        .group("person")
                        .vertex("v1")
                        .property("string-prop", "marko")
                        .property("int-prop", 29)
                        .property("double-prop", 0.4d)
                        .property("dateTime-prop", DateUtil.parseTime("2000-01-02 03:04:05"))
                        .property("long-prop", 10L)
                        .property("float-prop", 0.3f)
                        .property("boolean-prop", false)
                        .property("byte-prop", 1)
                        .property("short-prop", 1)
                        .property("char-prop", "K")
                        .property("date-prop", "2000-01-01")
                        .property("localDate-prop", "2000-01-01")
                        .property("localDateTime-prop", "2015-07-04T19:32:24")
                        .property("duration-prop", "P14DT16H12M")
                        .property("point-prop", "latitude:'13.10' longitude:'56.41'")
                        .build()
        );
    }

    @Test
    private void shouldGenerateEdgeWithPropertiesWithCorrectTypes() throws IOException {
        // Given
        final Iterable<String> lines = getInputData("EdgeWithPropertiesOfMultipleTypes.csv");

        // When
        final T generator = getGenerator();
        Iterable<Edge> edge = (Iterable<Edge>) generator.apply(lines);

        // Then
        assertThat(edge).containsExactly(
                new Edge.Builder()
                        .group("created")
                        .source("v1")
                        .dest("v2")
                        .directed(true)
                        .property("edge-id", "e1")
                        .property("int-prop", 4)
                        .property("double-prop", 0.4d)
                        .property("dateTime-prop", DateUtil.parseTime("2000-01-02 03:04:05"))
                        .property("long-prop", 10L)
                        .property("float-prop", 0.3f)
                        .property("boolean-prop", false)
                        .property("byte-prop", 1)
                        .property("short-prop", 1)
                        .property("char-prop", "K")
                        .property("date-prop", "2000-01-01")
                        .property("localDate-prop", "2000-01-01")
                        .property("localDateTime-prop", "2015-07-04T19:32:24")
                        .property("duration-prop", "P14DT16H12M")
                        .property("point-prop", "latitude:'13.10' longitude:'56.41'")
                        .build()
        );
    }

    @Test
    private void shouldGenerateBasicEntitesAndEdgesCsvFromNeo4jExport() throws IOException {
        // Given
        final Iterable<String> lines = getInputData("Neo4jBasicEntitiesAndEdge.csv");

        // When
        final T generator = getGenerator(true, ',', "");
        final Iterable<Element> elements = (Iterable<Element>) generator.apply(lines);

        // Then
        assertThat(elements).containsExactly(
                new Entity("person", "v1"),
                new Entity("software", "v2"),
                new Edge.Builder()
                        .group("created")
                        .source("v1")
                        .dest("v2")
                        .directed(true)
                        .property("edge-id", "e1")
                        .build()
        );
    }

    @Test
    private void shouldThrowErrorUnsupportedHeaderType() throws IOException {
        // Given
        final Iterable<String> lines = getInputData("EntityWithPropertyWithUnsupportedType.csv");

        // When
        final T generator = getGenerator();

        // Then
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(()-> generator.apply(lines))
                .withMessage("Unsupported Type: Array");
    }
}
