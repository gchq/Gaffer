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

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.koryphe.util.DateUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OpenCypherCsvElementGeneratorTest {
    private Iterable<String> getInputData(String filename) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/openCypherCSVs/" + filename)))) {
            return reader.lines().collect(Collectors.toList());
        }
    }

    private OpenCypherCsvElementGenerator getGenerator(Iterable<String> lines, boolean trim, char delimiter, String nullString) {
        String header = lines.iterator().next();
        OpenCypherCsvElementGenerator generator = new OpenCypherCsvElementGenerator.Builder()
                .header(header)
                .delimiter(delimiter)
                .trim(trim)
                .nullString(nullString)
                .build();
        return generator;
    }

    private OpenCypherCsvElementGenerator getGenerator(Iterable<String> lines) {
        return getGenerator(lines, true, ',', "");
    }

    @Test
    void shouldBuildGenerator() throws IOException {
        //Given
        Iterable<String> lines = getInputData("openCypherBasicEntities.csv");

        //When
        OpenCypherCsvElementGenerator generator = getGenerator(lines);

        //Then
        assertThat(generator.getDelimiter()).isEqualTo(',');
        assertThat(generator.getNullString()).isEqualTo("");
        assertThat(generator.getTrim()).isEqualTo(true);
        assertThat(generator.getHeader()).isEqualTo(":ID,:LABEL");
    }

    @Test
    void shouldGenerateBasicEntity() throws IOException {
        //Given
        Iterable<String> lines = getInputData("openCypherBasicEntities.csv");

        //When
        OpenCypherCsvElementGenerator generator = getGenerator(lines);
        Iterable<Element> elements = (Iterable<Element>) generator.apply(lines);

        //Then
        assertThat(elements).containsExactly(
            new Entity("person", "v1"),
            new Entity("software", "v2")
        );
    }

    @Test
    void shouldGenerateBasicEdge() throws IOException {
        //Given
        Iterable<String> lines = getInputData("openCypherBasicEdge.csv");

        //When
        OpenCypherCsvElementGenerator generator = getGenerator(lines);
        Iterable<Edge> edge = (Iterable<Edge>) generator.apply(lines);

        //Then
        assertThat(edge).containsExactly(
            new Edge("created", "v1", "v2", true)
        );
    }

    @Test
    void shouldGenerateBasicEntityFromPipeDelimitedCsv() throws IOException {
        //Given
        Iterable<String> lines = getInputData("openCypherBasicEntityPipeDelimited.csv");

        //When
        OpenCypherCsvElementGenerator generator = getGenerator(lines, true, '|', "");
        Iterable<Element> elements = (Iterable<Element>) generator.apply(lines);

        //Then
        assertThat(elements).containsExactly(
            new Entity("person", "v1"),
            new Entity("software", "v2")
        );
    }

    @Test
    void shouldGenerateBasicEntityFromCsvWithWhiteSpacePadding() throws IOException {
        //Given
        Iterable<String> lines = getInputData("openCypherBasicEntityPaddingSpaces.csv");

        //When
        OpenCypherCsvElementGenerator generator = getGenerator(lines);
        Iterable<Element> elements = (Iterable<Element>) generator.apply(lines);

        //Then
        assertThat(elements).containsExactly(
            new Entity("person", "v1"),
            new Entity("software", "v2")
        );
    }

    @Test
    void shouldGenerateEdgeWithID() throws IOException {
        //Given
        Iterable<String> lines = getInputData("openCypherEdgeWithID.csv");

        //When
        OpenCypherCsvElementGenerator generator = getGenerator(lines);
        Iterable<Edge> edge = (Iterable<Edge>) generator.apply(lines);

        //Then
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
    void shouldGenerateBasicEntityFromCsvWithValuesSurroundedByDoubleQuotes() throws IOException {
        //Given
        Iterable<String> lines = getInputData("openCypherBasicEntityQuotedValues.csv");

        //When
        OpenCypherCsvElementGenerator generator = getGenerator(lines);
        Iterable<Element> elements = (Iterable<Element>) generator.apply(lines);

        //Then
        assertThat(elements).containsExactly(
            new Entity("person", "v1"),
            new Entity("software", "v2")
        );
    }

    @Test
    void shouldGenerateBasicEntitiesAndEdgesCsv() throws IOException {
        //Given
        Iterable<String> lines = getInputData("openCypherBasicEntitiesAndEdges.csv");

        //When
        OpenCypherCsvElementGenerator generator = getGenerator(lines);
        Iterable<Element> elements = (Iterable<Element>) generator.apply(lines);

        //Then
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
    void shouldGenerateEntityWithPropertiesNoTypes() throws IOException {
        //Given
        Iterable<String> lines = getInputData("openCypherEntityWithPropertiesNoTypes.csv");

        //When
        OpenCypherCsvElementGenerator generator = getGenerator(lines);
        Iterable<Element> elements = (Iterable<Element>) generator.apply(lines);

        //Then
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
    void shouldGenerateEntityWithPropertiesWithCorrectTypes() throws IOException {
        //Given
        Iterable<String> lines = getInputData("openCypherEntityWithPropertiesOfMultipleTypes.csv");

        //When
        OpenCypherCsvElementGenerator generator = getGenerator(lines);
        Iterable<Element> elements = (Iterable<Element>) generator.apply(lines);

        //Then
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
    void shouldGenerateEdgeWithPropertiesWithCorrectTypes() throws IOException {
        //Given
        Iterable<String> lines = getInputData("openCypherEdgeWithPropertiesOfMultipleTypes.csv");

        //When
        OpenCypherCsvElementGenerator generator = getGenerator(lines);
        Iterable<Edge> edge = (Iterable<Edge>) generator.apply(lines);

        //Then
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
    void shouldGenerateBasicEntitesAndEdgesCsvFromNeo4jExport() throws IOException {
        //Given
        Iterable<String> lines = getInputData("openCypherBasicEntities&EdgeFromNeo4jExport.csv");

        //When
        OpenCypherCsvElementGenerator generator = getGenerator(lines);
        Iterable<Element> elements = (Iterable<Element>) generator.apply(lines);

        //Then
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
    void shouldThrowErrorUnsupportedHeaderType() throws IOException {
        //Given
        Iterable<String> lines = getInputData("openCypherEntityWithPropertyWithUnsupportedType.csv");

        //When
        OpenCypherCsvElementGenerator generator = getGenerator(lines);
        Exception exception = assertThrows(RuntimeException.class, () -> {
            generator.apply(lines);
        });

        String expectedMessage = "Unsupported Type: Array";
        String actualMessage = exception.getMessage();

        //Then
        assertThat(expectedMessage).isEqualTo(actualMessage);
    }

}
