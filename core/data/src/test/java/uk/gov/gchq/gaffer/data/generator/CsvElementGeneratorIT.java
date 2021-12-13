/*
 * Copyright 2016-2021 Crown Copyright
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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CsvElementGeneratorIT {
    private static final String GENERATOR_FILE = "/roadTraffic/roadTrafficGenerator.json";
    private static final String INPUT_DATA_FILE = "/roadTraffic/roadTrafficData.csv";


    private CsvElementGenerator deserialiseGenerator() throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(GENERATOR_FILE)))) {
            String content = reader.lines().collect(Collectors.joining("\n"));
            return JSONSerialiser.deserialise(content, CsvElementGenerator.class);
        }
    }

    private Iterable<String> getInputData() throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(INPUT_DATA_FILE)))) {
            return reader.lines().collect(Collectors.toList());
        }
    }

    @Test
    public void shouldGenerateElementsWithoutError() throws IOException {
        // Given
        CsvElementGenerator generator = deserialiseGenerator();
        Iterable<String> inputData = getInputData();

        // When
        Iterable<? extends Element> generated = generator.apply(inputData);

        // Then no exceptions
        for (Element element : generated) {
            // do nothing
        }
    }

    @Test
    public void shouldGenerateAllElementTypes() throws IOException {
        // Given
        CsvElementGenerator generator = deserialiseGenerator();
        Iterable<String> inputData = getInputData();

        // When
        Iterable<? extends Element> generated = generator.apply(inputData);

        // Then
        HashSet<String> expectedGroups = Sets.newHashSet("Cardinality", "JunctionUse", "RoadUse", "RoadHasJunction", "JunctionLocatedAt", "RegionContainsLocation", "LocationContainsRoad");
        HashSet<String> actual = new HashSet<>();

        generated.forEach(e -> actual.add(e.getGroup()));

        assertEquals(expectedGroups, actual);
    }

    @ParameterizedTest
    @CsvSource({
            "Cardinality,143",
            "JunctionUse,26",
            "RoadUse,13",
            "RoadHasJunction,26",
            "RegionContainsLocation,13",
            "LocationContainsRoad,13",
            "JunctionLocatedAt,13"
    })
    public void shouldGenerateCorrectNumberOfElementTypes(String group, Integer expectedNumberOfElements) throws IOException {
        // Given
        CsvElementGenerator generator = deserialiseGenerator();
        Iterable<String> inputData = getInputData();

        // When
        Iterable<? extends Element> generated = generator.apply(inputData);

        // Then
        int count = 0;
        for (Element element : generated) {
            if (element.getGroup().equals(group))
                count++;
        }

        assertEquals(expectedNumberOfElements, count);
    }

    @Test
    public void shouldGenerateOneRegionAndFourLocations() throws IOException {
        // Given
        CsvElementGenerator generator = deserialiseGenerator();
        Iterable<String> inputData = getInputData();

        // When
        Iterable<? extends Element> generated = generator.apply(inputData);

        // Then
        HashSet<Element> expected = Sets.newHashSet(createRegionContainsLocationEdge("Newport"),
                createRegionContainsLocationEdge("Cardiff"),
                createRegionContainsLocationEdge("Isle of Anglesey"),
                createRegionContainsLocationEdge("Conwy")
                );
        HashSet<Element> actual = new HashSet<>(); // I don't care about duplications for the sake of this test

        for (Element element : generated) {
            if (element.getGroup().equals("RegionContainsLocation"))
                actual.add(element);
        }

        assertEquals(expected, actual);
    }

    @Test
    public void shouldCorrectlyGenerateCardinalityEntities() throws IOException {
        CsvElementGenerator generator = deserialiseGenerator();
        Iterable<String> inputData = getInputData();

        // When
        List<? extends Element> cardinalityEntities = Lists.newArrayList(generator.apply(inputData))
                .stream()
                .filter(e -> e.getGroup().equals("Cardinality"))
                .collect(Collectors.toList());
        // Then

        cardinalityEntities.forEach(e -> {
                    assertEquals(TreeSet.class, e.getProperty("edgeGroup").getClass());
                    assertEquals(Long.class, e.getProperty("count").getClass());
        });
    }

    @Test
    public void shouldGenerateFourRoadsAndEightJunctions() throws IOException {
        // Given
        CsvElementGenerator generator = deserialiseGenerator();
        Iterable<String> inputData = getInputData();

        // When
        Iterable<? extends Element> generated = generator.apply(inputData);

        // Then
        HashSet<Element> expected = new HashSet<>();
        expected.addAll(createRoadHasJunctionEdges("M4", "28", "27"));
        expected.addAll(createRoadHasJunctionEdges("A5", "A4080", "A5114"));
        expected.addAll(createRoadHasJunctionEdges("A48", "A4232 / A4050", "A4161"));
        expected.addAll(createRoadHasJunctionEdges("A55", "LA Boundary", "A547"));
        HashSet<Element> actual = new HashSet<>(); // I don't care about duplications for the sake of this test

        for (Element element : generated) {
            if (element.getGroup().equals("RoadHasJunction"))
                actual.add(element);
        }

        assertEquals(expected, actual);
    }

    private Collection<? extends Element> createRoadHasJunctionEdges(String road, String startJunction, String endJunction) {
        return Sets.newHashSet(
                new Edge.Builder()
                        .source(road)
                        .dest(road + ":" + startJunction)
                        .group("RoadHasJunction")
                        .directed(true)
                        .build(),
                new Edge.Builder()
                        .source(road)
                        .dest(road + ":" + endJunction)
                        .group("RoadHasJunction")
                        .directed(true)
                        .build()
                );
    }

    public Element createRegionContainsLocationEdge(String location) {
        return new Edge.Builder()
                .group("RegionContainsLocation")
                .source("Wales")
                .dest(location)
                .directed(true)
                .build();
    }
}
