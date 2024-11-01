/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple.merge;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.impl.binaryoperator.CollectionIntersect;
import uk.gov.gchq.koryphe.impl.binaryoperator.Or;
import uk.gov.gchq.koryphe.impl.binaryoperator.Product;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringDeduplicateConcat;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

import static uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties.PROP_DEFAULT_MERGE_ELEMENTS;
import static uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties.PROP_MERGE_CLASS_BOOLEAN;
import static uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties.PROP_MERGE_CLASS_COLLECTION;
import static uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties.PROP_MERGE_CLASS_MAP;
import static uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties.PROP_MERGE_CLASS_NUMBER;
import static uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties.PROP_MERGE_CLASS_STRING;

class DefaultResultAccumulatorTest {

    static Stream<Arguments> commonDataArgs() {
        return Stream.of(
            Arguments.of(1, 2, 3),
            Arguments.of("s1", "s2", "s1,s2"),
            Arguments.of(true, false, false),
            Arguments.of(true, true, true),
            Arguments.of(
                new ArrayList<>(Arrays.asList("a", "b")),
                new ArrayList<>(Arrays.asList("c", "d")),
                Arrays.asList("a", "b", "c", "d")),
            Arguments.of(
                Stream.of(new SimpleEntry<String, String>("a", "b"))
                        .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue)),
                Stream.of(new SimpleEntry<String, String>("a", "c"))
                        .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue)),
                Stream.of(new SimpleEntry<String, String>("a", "c"))
                        .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue))));
    }

    static Stream<Arguments> customMergeDataArgs() {
        return Stream.of(
            Arguments.of(2, 3, 6),
            Arguments.of("s1", "s1", "s1"),
            Arguments.of(true, false, true),
            Arguments.of(true, true, true),
            Arguments.of(
                new ArrayList<>(Arrays.asList("a", "c")),
                new ArrayList<>(Arrays.asList("c", "d")),
                Arrays.asList("c")),
            Arguments.of(
                Stream.of(new SimpleEntry<String, String>("a", "b"))
                        .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue)),
                Stream.of(new SimpleEntry<String, String>("a", "c"))
                        .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue)),
                Stream.of(new SimpleEntry<String, String>("a", "b,c"))
                        .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue))));
    }

    @DisplayName("Should provide default merge methods for common data types")
    @ParameterizedTest
    @MethodSource("commonDataArgs")
    <T> void shouldAccumulatePrimitiveDataByDefault(T result1, T result2, T expected) {
        FederatedResultAccumulator<T> accumulator = new DefaultResultAccumulator<>();
        assertThat(accumulator.apply(result1, result2)).isEqualTo(expected);
    }

    @DisplayName("Should allow customising the merge operators for common data types")
    @ParameterizedTest
    @MethodSource("customMergeDataArgs")
    <T> void shouldAllowCustomOperatorsForPrimitiveData(T result1, T result2, T expected) {
        // Set properties to update the operators
        java.util.Properties properties = new java.util.Properties();
        properties.setProperty(PROP_MERGE_CLASS_NUMBER, Product.class.getName());
        properties.setProperty(PROP_MERGE_CLASS_STRING, StringDeduplicateConcat.class.getName());
        properties.setProperty(PROP_MERGE_CLASS_BOOLEAN, Or.class.getName());
        properties.setProperty(PROP_MERGE_CLASS_COLLECTION, CollectionIntersect.class.getName());
        properties.setProperty(PROP_MERGE_CLASS_MAP, StringConcat.class.getName());

        // Init the accumulator with custom properties
        FederatedResultAccumulator<T> accumulator = new DefaultResultAccumulator<>(properties);
        assertThat(accumulator.apply(result1, result2)).isEqualTo(expected);
    }

    @Test
    void shouldNotDoElementAggregationByDefault() {
        Iterable<Entity> iter1 = () -> Arrays.asList(new Entity("person", "1")).iterator();
        Iterable<Entity> iter2 = () -> Arrays.asList(new Entity("person", "1")).iterator();
        Iterable<Entity> expected = () -> Arrays.asList(
            new Entity("person", "1"),
            new Entity("person", "1")).iterator();

        FederatedResultAccumulator<Iterable<Entity>> accumulator = new DefaultResultAccumulator<>();
        assertThat(accumulator.apply(iter1, iter2)).containsExactlyElementsOf(expected);
    }

    @Test
    void shouldSetElementAggregationFromProperties() {
        // Given
        FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(PROP_DEFAULT_MERGE_ELEMENTS, "true");

        // When/Then
        FederatedResultAccumulator<?> accumulator = new DefaultResultAccumulator<>(properties.getProperties());
        assertThat(accumulator.aggregateElements()).isTrue();
    }

    @Test
    void shouldProvideElementAggregationBySchema() {
        // Given
        Schema schema = Schema.fromJson(StreamUtil.openStreams(this.getClass(), "/modern/schema"));
        // Add some properties so can be sure aggregation has happened
        Properties iter1EntityProps = new Properties();
        iter1EntityProps.put("name", "marko");
        Properties iter2EntityProps = new Properties();
        iter2EntityProps.put("age", 29);

        // Add the same vertex but with different properties
        Iterable<Entity> iter1 = () -> Arrays.asList(new Entity("person", "1", iter1EntityProps)).iterator();
        Iterable<Entity> iter2 = () -> Arrays.asList(new Entity("person", "1", iter2EntityProps)).iterator();

        // We are expecting merged properties
        Properties mergedProperties = new Properties();
        mergedProperties.putAll(iter1EntityProps);
        mergedProperties.putAll(iter2EntityProps);
        Iterable<Entity> expected = () -> Arrays.asList(new Entity("person", "1", mergedProperties)).iterator();

        // When
        FederatedResultAccumulator<Iterable<Entity>> accumulator = new DefaultResultAccumulator<>();
        accumulator.setAggregateElements(true);
        accumulator.setSchema(schema);

        // Then
        assertThat(accumulator.apply(iter1, iter2)).containsExactlyElementsOf(expected);
    }

    @Test
    void shouldNotAggregateDifferentElementsOfSameGroup() {
        // Given
        Schema schema = Schema.fromJson(StreamUtil.openStreams(this.getClass(), "/modern/schema"));
        // Add some properties so can be sure aggregation has not happened
        Properties iter1EntityProps = new Properties();
        iter1EntityProps.put("name", "marko");
        Properties iter2EntityProps = new Properties();
        iter2EntityProps.put("name", "vadas");

        // Add different vertexes from the same group
        Entity entity1 = new Entity("person", "1", iter1EntityProps);
        Entity entity2 = new Entity("person", "2", iter2EntityProps);
        Iterable<Entity> iter1 = () -> Arrays.asList(entity1).iterator();
        Iterable<Entity> iter2 = () -> Arrays.asList(entity2).iterator();

        // We are just expecting chained iterable
        Iterable<Entity> expected = () -> Arrays.asList(entity1, entity2).iterator();

        // When
        FederatedResultAccumulator<Iterable<Entity>> accumulator = new DefaultResultAccumulator<>();
        accumulator.setAggregateElements(true);
        accumulator.setSchema(schema);

        // Then
        assertThat(accumulator.apply(iter1, iter2)).containsExactlyElementsOf(expected);
    }
}
