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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.iterable.LimitedCloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.StreamIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.TransformIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.TransformOneToManyIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.Validator;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTupleDefinition;
import uk.gov.gchq.gaffer.data.element.function.PropertiesFilter;
import uk.gov.gchq.gaffer.data.element.function.PropertiesTransformer;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.koryphe.impl.function.Concat;
import uk.gov.gchq.koryphe.impl.function.FunctionChain;
import uk.gov.gchq.koryphe.impl.function.Identity;
import uk.gov.gchq.koryphe.impl.function.SetValue;
import uk.gov.gchq.koryphe.impl.function.ToList;
import uk.gov.gchq.koryphe.impl.function.ToLong;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunction;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class CsvElementGeneratorTest {

    public void assertElementGenerated(Element expected, Iterable<? extends  Element> generated) {
        int elementsGenerated = 0;
        for (Element element : generated) {
            assertEquals(expected, element);
            elementsGenerated++;
        }
        assertEquals(1, elementsGenerated);
    }

    @Test
    public void shouldCreateAnEdgeWithNoProperties() {
        // Given
        List<String> header = Arrays.asList("local_authority_name", "road_name");
        List<String> lines = new ArrayList<>();
        lines.add("local_authority_name,road_name");
        lines.add("Cardiff,M4");

        // When
        CsvElementGenerator generator = new CsvElementGenerator()
                .header(header)
                .firstRow(1)
                .element(new ElementTupleDefinition()
                        .group("LocationContainsRoad")
                        .directed(true)
                        .source("Cardiff")
                        .destination("M4"));

        Iterable<? extends Element> generated = generator.apply(lines);

        // Then
        Edge expected = new Edge("LocationContainsRoad", "Cardiff", "M4", true);
        assertElementGenerated(expected, generated);
    }

    @Test
    public void shouldBeAbleToCreateAnElementWithStringProperties() {
        // Given
        List<String> header = Arrays.asList("start_junction", "end_junction", "heading");
        List<String> lines = new ArrayList<>();
        lines.add("start_junction,end_junction,heading");
        lines.add("1,2,East");

        // When
        CsvElementGenerator generator = new CsvElementGenerator()
                .header(header)
                .firstRow(1)
                .element(new ElementTupleDefinition()
                    .source("start_junction")
                    .destination("end_junction")
                    .directed(true)
                    .group("JunctionUse")
                    .property("direction", "heading"));

        Iterable<? extends Element> generated = generator.apply(lines);

        // Then
        Edge expected = new Edge("JunctionUse", "1", "2", true);
        expected.putProperty("direction", "East");

        assertElementGenerated(expected, generated);
    }

    @Test
    public void shouldBeAbleToCreateMoreThanOneElementForAGivenGroup() {
        // Given
        List<String> lines = new ArrayList<>();
        lines.add("start_junction,end_junction,road");
        lines.add("1,2,M32");

        // When
        CsvElementGenerator generator = new CsvElementGenerator()
                .header("start_junction", "end_junction", "road")
                .firstRow(1)
                .element(new ElementTupleDefinition()
                        .source("road")
                        .destination("start_junction")
                        .directed(false)
                        .group("RoadHasJunction"))
                .element(new ElementTupleDefinition()
                        .source("road")
                        .destination("end_junction")
                        .directed(false)
                        .group("RoadHasJunction"));

        Iterable<? extends Element> generated = generator.apply(lines);

        // Then
        List<Element> generatedElements = new ArrayList<>();
        generated.forEach(generatedElements::add);

        Edge startEdge = new Edge("RoadHasJunction", "M32", "1", false);
        Edge endEdge = new Edge("RoadHasJunction", "M32", "2", false);

        assertTrue(generatedElements.contains(startEdge), "Expected start edge to be generated");
        assertTrue(generatedElements.contains(endEdge), "Expected end edge to be generated");
        assertEquals(2, generatedElements.size(), "Expected exactly two elements to be generated");
    }

    @Test
    public void shouldCreateElementsWhichHaveNonStringProperties() {
        // Given
        List<String> lines = new ArrayList<>();
        lines.add("road,count");
        lines.add("M32,2");

        // When
        CsvElementGenerator generator = new CsvElementGenerator()
                .header("road", "count")
                .firstRow(1)
                .transformer(new PropertiesTransformer.Builder()
                        .select("count")
                        .execute(new ToLong())
                        .project("count")
                        .build())
                .element(new ElementTupleDefinition()
                        .group("RoadCounts")
                        .vertex("road")
                        .property("count"));

        Iterable<? extends Element> generated = generator.apply(lines);

        // Then
        Entity expected = new Entity("RoadCounts", "M32");
        expected.putProperty("count", 2L);

        assertElementGenerated(expected, generated);
    }

    @Test
    public void shouldIgnoreHeaderIfShuffled() {
        List<String> csv = new ArrayList<>();
        csv.add("\"M32\",7.3");
        csv.add("\"road\",\"length\"");
        csv.add("\"A38\", 8.6");

        // When
        CsvElementGenerator generator = new CsvElementGenerator()
                .header(Lists.newArrayList("road", "length"))
                .quoted()
                .transformer(new PropertiesTransformer.Builder()
                        .select("length")
                        .execute(new ToDouble())
                        .project("length")
                        .build()
                )
                .element(new ElementTupleDefinition()
                        .vertex("road")
                        .group("Road")
                        .property("length")
                );

        // This will throw exception if it tries to parse the header
        Iterable<? extends Element> generated = generator.apply(csv);

        // Then
        List<Element> expected = new ArrayList<>();
        expected.add(new Entity.Builder().group("Road").vertex("M32").property("length", 7.3).build());
        expected.add(new Entity.Builder().group("Road").vertex("A38").property("length", 8.6).build());

        List<Element> actual = new ArrayList<>();
        generated.forEach(actual::add);

        assertEquals(expected, actual);
    }

    @Test
    public void shouldBeAbleToCreateAPropertyDerivedFromMultipleCsvValues() {
        // Given
        List<String> lines = new ArrayList<>();
        lines.add("road,two_wheeled_motor_vehicles,cars_and_taxis,buses_and_coaches,hgvs,lgvs,bicycles");
        lines.add("A38,2,50,5,8,10,2");

        // When
        CsvElementGenerator generator = new CsvElementGenerator()
                .header("road", "two_wheeled_motor_vehicles", "cars_and_taxis", "buses_and_coaches", "hgvs", "lgvs", "bicycles")
                .firstRow(1)
                .transformer(new PropertiesTransformer.Builder()
                        .select("two_wheeled_motor_vehicles")
                        .execute(new ToLong())
                        .project("motorbikes")
                        .select("cars_and_taxis")
                        .execute(new ToLong())
                        .project("cars")
                        .select("buses_and_coaches")
                        .execute(new ToLong())
                        .project("buses")
                        .select("hgvs")
                        .execute(new ToLong())
                        .project("lorries")
                        .select("lgvs")
                        .execute(new ToLong())
                        .project("vans")
                        .select("bicycles")
                        .execute(new ToLong())
                        .project("bicycles")
                        .select("motorbikes", "bicycles", "cars", "buses", "vans", "lorries")
                        .execute(new FunctionChain.Builder<Iterable<String>, FreqMap>().execute(new ToList()).execute(new CountsToFreqMap(Arrays.asList("Motorbikes", "Bicycles", "Cars", "Buses", "Vans", "Lorries"))).build())
                        .project("countByVehicleType")
                        .build())
                .element(new ElementTupleDefinition()
                        .vertex("road")
                        .group("RoadUse")
                        .property("countByVehicleType"));

        Iterable<? extends Element> generated = generator.apply(lines);

        // Then
        FreqMap roadUse = new FreqMap();
        roadUse.upsert("Motorbikes", 2L);
        roadUse.upsert("Bicycles", 2L);
        roadUse.upsert("Cars", 50L);
        roadUse.upsert("Buses", 5L);
        roadUse.upsert("Vans", 10L);
        roadUse.upsert("Lorries", 8L);
        Entity expected = new Entity("RoadUse", "A38");
        expected.putProperty("countByVehicleType", roadUse);

        assertElementGenerated(expected, generated);
    }

    @Test
    public void shouldBeAbleToCreateCardinalityEntities() {
        // Given
        List<String> header = Arrays.asList("road","start_junction","end_junction");
        List<String> lines = new ArrayList<>();
        lines.add("M5,1,2");

        // When
        CsvElementGenerator generator = new CsvElementGenerator()
                .header(header)
                .transformer(new PropertiesTransformer.Builder()
                        .select("start_junction", "end_junction")
                        .execute(new IterableToHyperLogLogPlus())
                        .project("hllp_road_has_junction")
                        .select("start_junction")
                        .execute(new ToHyperLogLogPlus())
                        .project("hllp_junction_use_end")
                        .select("end_junction")
                        .execute(new ToHyperLogLogPlus())
                        .project("hllp_junction_use_start")
                        .build()
                )
                .element(new ElementTupleDefinition()
                        .group("Cardinality")
                        .vertex("start_junction")
                        .property("edgeGroup", Sets.newHashSet("JunctionUse"))
                        .property("hllp", "hllp_junction_use_start"))
                .element(new ElementTupleDefinition()
                        .vertex("end_junction")
                        .group("Cardinality")
                        .property("edgeGroup", Sets.newHashSet("JunctionUse"))
                        .property("hllp", "hllp_junction_use_end"))
                .element(new ElementTupleDefinition()
                        .vertex("road")
                        .group("Cardinality")
                        .property("edgeGroup", Sets.newHashSet("RoadHasJunction"))
                        .property("hllp", "hllp_road_has_junction"));

        Iterable<? extends Element> generated = generator.apply(lines);
        List<Element> generatedElements = new ArrayList<>();
        generated.forEach(generatedElements::add);

        // Then
        HyperLogLogPlus startJunctionHllp = new HyperLogLogPlus(5, 5);
        HyperLogLogPlus endJunctionHllp = new HyperLogLogPlus(5, 5);
        HyperLogLogPlus roadHllp = new HyperLogLogPlus(5, 5);

        startJunctionHllp.offer("2");
        endJunctionHllp.offer("1");
        roadHllp.offer("1");
        roadHllp.offer("2");

        List<Entity> expected = Arrays.asList(
                new Entity.Builder()
                        .vertex("1")
                        .property("hllp", startJunctionHllp)
                        .property("edgeGroup", Sets.newHashSet("JunctionUse"))
                        .group("Cardinality")
                        .build(),
                new Entity.Builder()
                        .vertex("2")
                        .property("hllp", endJunctionHllp)
                        .property("edgeGroup", Sets.newHashSet("JunctionUse"))
                        .group("Cardinality")
                        .build(),
                new Entity.Builder()
                        .vertex("M5")
                        .property("hllp", roadHllp)
                        .property("edgeGroup", Sets.newHashSet("RoadHasJunction"))
                        .group("Cardinality")
                        .build()
        );

        assertEquals(3, generatedElements.size());
        for (int i = 0; i < generatedElements.size(); i++) {
            assertEquals(expected.get(i).getVertex(), ((Entity) (generatedElements.get(i))).getVertex());
            assertEquals(expected.get(i).getGroup(), generatedElements.get(i).getGroup());
            assertEquals(expected.get(i).getProperty("edgeGroup"), generatedElements.get(i).getProperty("edgeGroup"));
            assertEquals(((HyperLogLogPlus) expected.get(i).getProperty("hllp")).cardinality(), ((HyperLogLogPlus) generatedElements.get(i).getProperty("hllp")).cardinality());
        }

    }

    @Test
    public void shouldJsonSerialise() throws SerialisationException {
        // Given
        List<String> header = Arrays.asList("road","start_junction","end_junction", "heading", "latitude", "longitude", "count");
        CsvElementGenerator generator = new CsvElementGenerator()
                .header(header)
                .transformer(new PropertiesTransformer.Builder()
                        .select("start_junction", "end_junction")
                        .execute(new IterableToHyperLogLogPlus())
                        .project("hllp_road_has_junction")
                        .select("start_junction")
                        .execute(new ToHyperLogLogPlus())
                        .project("hllp_junction_use_end")
                        .select("end_junction")
                        .execute(new ToHyperLogLogPlus())
                        .project("hllp_junction_use_start")
                        .select("count")
                        .execute(new ToLong())
                        .project("count")
                        .select("latitude", "longitude")
                        .execute(new Concat())
                        .project("latLong")
                        .select()
                        .execute(new SetValue(Sets.newHashSet("JunctionUse")))
                        .project("edge_group_junction_use")
                        .build()
                )
                .element(new ElementTupleDefinition()
                        .source("road")
                        .destination("start_junction")
                        .group("RoadHasJunction")
                        .directed(true)
                )
                .element(new ElementTupleDefinition()
                        .source("road")
                        .destination("end_junction")
                        .group("RoadHasJunction")
                        .directed(true)
                )
                .element(new ElementTupleDefinition()
                        .source("start_junction")
                        .destination("end_junction")
                        .group("RoadUse")
                        .directed(true)
                        .property("heading")
                        .property("count")
                )
                .element(new ElementTupleDefinition()
                        .group("JunctionUse")
                        .vertex("start_junction")
                        .property("latLong")
                )
                .element(new ElementTupleDefinition()
                        .group("Cardinality")
                        .vertex("start_junction")
                        .property("edgeGroup", "edge_group_junction_use")
                        .property("hllp", "hllp_junction_use_start"));

        // When
        String serialised = new String(JSONSerialiser.serialise(generator, true));
        String expectedSerialised;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/csvGenerator.json")))) {
            StringBuilder sb = new StringBuilder();
            reader.lines().forEach(line -> sb.append(line).append('\n'));
            sb.deleteCharAt(sb.lastIndexOf("\n"));
            expectedSerialised = sb.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Then
        assertEquals(expectedSerialised, serialised);
    }

    @Test
    public void shouldHandleQuotedValues() {
        // Given
        List<String> csv = new ArrayList<>();
        csv.add("\"road\",\"length\"");
        csv.add("\"M32\",8.6");
        csv.add("\"A38\", 8.0");

        // When
        CsvElementGenerator generator = new CsvElementGenerator()
                .firstRow(1)
                .header(Lists.newArrayList("road", "length"))
                .quoted()
                .transformer(new PropertiesTransformer.Builder()
                        .select("length")
                        .execute(new ToDouble())
                        .project("length")
                        .build()
                )
                .element(new ElementTupleDefinition()
                        .vertex("road")
                        .group("Road")
                        .property("length")
                );

        Iterable<? extends Element> generated = generator.apply(csv);

        // Then
        List<Element> expected = new ArrayList<>();
        expected.add(new Entity.Builder().group("Road").vertex("M32").property("length", 8.6).build());
        expected.add(new Entity.Builder().group("Road").vertex("A38").property("length", 8.0).build());

        List<Element> actual = new ArrayList<>();
        generated.forEach(actual::add);

        assertEquals(expected, actual);
    }

    @Test
    public void shouldThrowExceptionIfCsvIsInvalidAndSkipInvalidIsNotSet() {
        // Given
        List<String> csv = new ArrayList<>();
        csv.add("\"road\",\"length\"");
        csv.add("\"M32\","); // missing length
        csv.add("\"A38\", 8.0");

        // When
        CsvElementGenerator generator = new CsvElementGenerator()
                .firstRow(1)
                .header(Lists.newArrayList("road", "length"))
                .allFieldsRequired()
                .quoted()
                .transformer(new PropertiesTransformer.Builder()
                        .select("length")
                        .execute(new ToDouble())
                        .project("length")
                        .build()
                )
                .element(new ElementTupleDefinition()
                        .vertex("road")
                        .group("Road")
                        .property("length")
                );

        // Then
        try {
            Iterable<? extends Element> generated = generator.apply(csv);
            generated.forEach(element -> {});
            fail("Expected apply to throw exception");
        } catch (Exception e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldNotThrowExceptionIfCsvIsInvalidButSkipInvalidIsSet() {
        // Given
        List<String> csv = new ArrayList<>();
        csv.add("\"road\",\"length\"");
        csv.add("\"M32\","); // missing length
        csv.add("\"A38\", 8.0");

        // When
        CsvElementGenerator generator = new CsvElementGenerator()
                .firstRow(1)
                .header(Lists.newArrayList("road", "length"))
                .allFieldsRequired()
                .skipInvalid()
                .quoted()
                .transformer(new PropertiesTransformer.Builder()
                        .select("length")
                        .execute(new ToDouble())
                        .project("length")
                        .build()
                )
                .element(new ElementTupleDefinition()
                        .vertex("road")
                        .group("Road")
                        .property("length")
                );

        // Then no exceptions
        Iterable<? extends Element> generated = generator.apply(csv);
        ArrayList<Element> actual = new ArrayList<>();
        generated.forEach(actual::add);

        assertEquals(1, actual.size());
    }

    @Test
    @Disabled
    /**
     * Once this passes, the Deserialisation test can be enabled.
     */
    public void testTupleAdaptedFunction() {
        // Given
        TupleAdaptedFunction a = new TupleAdaptedFunction(new String[] {"thing"},
                new Identity(), new String[] {"anotherThing"});

        // When
        TupleAdaptedFunction b = new TupleAdaptedFunction(new String[] {"thing"},
                new Identity(), new String[] {"anotherThing"});

        // Then
        assertEquals(a, b);
    }

    @Test
    @Disabled
    /**
     * Ignored as Equality is hard to judge as TupleAdaptedFunction doesn't correctly
     * Invoke an Equals / Hashcode
     */
    public void shouldJsonDeserialise() throws SerialisationException {
        // Given
        List<String> header = Arrays.asList("road","start_junction","end_junction", "heading", "latitude", "longitude", "count");
        CsvElementGenerator generator = new CsvElementGenerator()
                .header(header)
                .transformer(new PropertiesTransformer.Builder()
                        .select("start_junction", "end_junction")
                        .execute(new IterableToHyperLogLogPlus())
                        .project("hllp_road_has_junction")
                        .select("start_junction")
                        .execute(new ToHyperLogLogPlus())
                        .project("hllp_junction_use_end")
                        .select("end_junction")
                        .execute(new ToHyperLogLogPlus())
                        .project("hllp_junction_use_start")
                        .select("count")
                        .execute(new ToLong())
                        .project("count")
                        .select("latitude", "longitude")
                        .execute(new Concat())
                        .project("latLong")
                        .select()
                        .execute(new SetValue(Sets.newHashSet("JunctionUse")))
                        .project("edge_group_junction_use")
                        .build()
                )
                .element(new ElementTupleDefinition()
                        .source("road")
                        .destination("start_junction")
                        .group("RoadHasJunction")
                        .directed(true)
                )
                .element(new ElementTupleDefinition()
                        .source("road")
                        .destination("end_junction")
                        .group("RoadHasJunction")
                        .directed(true)
                )
                .element(new ElementTupleDefinition()
                        .source("start_junction")
                        .destination("end_junction")
                        .group("RoadUse")
                        .directed(true)
                        .property("heading")
                        .property("count")
                )
                .element(new ElementTupleDefinition()
                        .group("JunctionUse")
                        .vertex("start_junction")
                        .property("latLong")
                )
                .element(new ElementTupleDefinition()
                        .group("Cardinality")
                        .vertex("start_junction")
                        .property("edgeGroup", "edge_group_junction_use")
                        .property("hllp", "hllp_junction_use_start"));

        // When
        String expectedSerialised;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/csvGenerator.json")))) {
            StringBuilder sb = new StringBuilder();
            reader.lines().forEach(line -> sb.append(line).append('\n'));
            sb.deleteCharAt(sb.lastIndexOf("\n"));
            expectedSerialised = sb.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        CsvElementGenerator deserialised = JSONSerialiser.deserialise(expectedSerialised, CsvElementGenerator.class);

        // Then
        assertEquals(generator, deserialised);
    }
}
