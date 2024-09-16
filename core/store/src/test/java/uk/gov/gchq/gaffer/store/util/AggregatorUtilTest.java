/*
 * Copyright 2017-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.store.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.ExampleFilterFunction;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaTest;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.Product;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static uk.gov.gchq.gaffer.data.util.ElementUtil.assertElementEquals;
import static uk.gov.gchq.gaffer.data.util.ElementUtil.assertElementEqualsIncludingMatchedVertex;


public class AggregatorUtilTest {
    @Test
    public void shouldThrowExceptionWhenIngestAggregatedIfSchemaIsNull() {
        // given
        final Schema schema = null;

        // When / Then
        assertThatIllegalArgumentException().isThrownBy(() -> AggregatorUtil.ingestAggregate(Collections.emptyList(), schema))
                .extracting("message").isNotNull();
    }

    @Test
    public void shouldIngestAggregateElementsWhenProvidedIterableCanOnlyBeConsumedOnce() {
        // given
        final Schema schema = Schema.fromJson(StreamUtil.openStreams(getClass(), "schema-groupby"));

        final List<Element> elements = Arrays.asList(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 1)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.NON_AGG_ENTITY)
                        .vertex("vertex1")
                        .property("count", 2)
                        .build());

        final Iterable<Element> onlyConsumingOnceIterable = new Iterable<Element>() {
            private boolean firstTime = true;

            @Override
            public Iterator<Element> iterator() {

                if (firstTime) {
                    firstTime = false;
                    return elements.iterator();
                }

                throw new NoSuchElementException();
            }
        };

        // when
        final Iterable<Element> aggregatedElements = AggregatorUtil.ingestAggregate(onlyConsumingOnceIterable, schema);

        // then
        assertElementEquals(elements, aggregatedElements);
    }

    @Test
    public void shouldIngestAggregateElementsWithNoGroupBy() {
        // given
        final Schema schema = Schema.fromJson(StreamUtil.openStreams(getClass(), "schema-groupby"));

        final List<Element> elements = Arrays.asList(
                new Entity.Builder()
                        .group(TestGroups.NON_AGG_ENTITY)
                        .vertex("vertex1")
                        .property("count", 1)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.NON_AGG_ENTITY)
                        .vertex("vertex1")
                        .property("count", 2)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 1)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 2)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex2")
                        .property("count", 10)
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 100)
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 200)
                        .build());

        final Set<Element> expected = Sets.newHashSet(
                new Entity.Builder()
                        .group(TestGroups.NON_AGG_ENTITY)
                        .vertex("vertex1")
                        .property("count", 1)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.NON_AGG_ENTITY)
                        .vertex("vertex1")
                        .property("count", 2)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 3)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex2")
                        .property("count", 10)
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 300)
                        .build());

        // when
        final Iterable<Element> aggregatedElements = AggregatorUtil.ingestAggregate(elements, schema);

        // then
        assertElementEquals(expected, aggregatedElements);
    }

    @Test
    public void shouldIngestAggregateElementsWithGroupBy() {
        // given
        final Schema schema = Schema.fromJson(StreamUtil.openStreams(getClass(), "schema-groupby"));
        final List<Element> elements = Arrays.asList(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 1)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 1)
                        .property("property2", "value1")
                        .property("visibility", "vis2")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 2)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 2)
                        .property("property2", "value2")
                        .property("visibility", "vis1")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 2)
                        .property("property2", "value2")
                        .property("visibility", "vis2")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 10)
                        .property("property2", "value2")
                        .property("visibility", "vis1")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex2")
                        .property("count", 20)
                        .property("property2", "value10")
                        .property("visibility", "vis1")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 100)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 100)
                        .property("property2", "value1")
                        .property("visibility", "vis2")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 100)
                        .property("property2", "value1")
                        .property("visibility", "vis2")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 200)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 1000)
                        .property("property2", "value2")
                        .property("visibility", "vis1")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 2000)
                        .property("property2", "value2")
                        .property("visibility", "vis1")
                        .build()

        );

        final Set<Element> expected = Sets.newHashSet(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 3)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 1)
                        .property("property2", "value1")
                        .property("visibility", "vis2")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 12)
                        .property("property2", "value2")
                        .property("visibility", "vis1")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 2)
                        .property("property2", "value2")
                        .property("visibility", "vis2")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex2")
                        .property("count", 20)
                        .property("property2", "value10")
                        .property("visibility", "vis1")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 300)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 200)
                        .property("property2", "value1")
                        .property("visibility", "vis2")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 3000)
                        .property("property2", "value2")
                        .property("visibility", "vis1")
                        .build());

        // when
        final Iterable<Element> aggregatedElements = AggregatorUtil.ingestAggregate(elements, schema);

        // then
        assertElementEquals(expected, aggregatedElements);
    }

    @Test
    public void shouldThrowExceptionWhenQueryAggregatedIfSchemaIsNull() {
        // given
        final Schema schema = null;
        final View view = new View();

        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> AggregatorUtil.queryAggregate(Collections.emptyList(), schema, view))
                .withMessageContaining("Schema");
    }

    @Test
    public void shouldThrowExceptionWhenQueryAggregatedIfViewIsNull() {
        // given
        final Schema schema = new Schema();
        final View view = null;

        // When / Then
        assertThatIllegalArgumentException()
                .isThrownBy(() -> AggregatorUtil.queryAggregate(Collections.emptyList(), schema, view))
                .withMessageContaining("View");
    }

    @Test
    public void shouldQueryAggregateElementsWithGroupBy() {
        // given
        final Schema schema = Schema.fromJson(StreamUtil.openStreams(getClass(), "schema-groupby"));
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .entity(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();

        final List<Element> elements = Arrays.asList(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 1)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 2)
                        .property("property2", "value1")
                        .property("visibility", "vis2")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 2)
                        .property("property2", "value2")
                        .property("visibility", "vis1")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 10)
                        .property("property2", "value2")
                        .property("visibility", "vis2")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex2")
                        .property("count", 20)
                        .property("property2", "value10")
                        .property("visibility", "vis1")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 100)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 200)
                        .property("property2", "value1")
                        .property("visibility", "vis2")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 1000)
                        .property("property2", "value2")
                        .property("visibility", "vis1")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 2000)
                        .property("property2", "value2")
                        .property("visibility", "vis2")
                        .build());

        final Set<Element> expected = Sets.newHashSet(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 15)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex2")
                        .property("count", 20)
                        .property("property2", "value10")
                        .property("visibility", "vis1")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 3300)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build());

        // when
        final Iterable<Element> aggregatedElements = AggregatorUtil.queryAggregate(elements, schema, view);

        // then
        assertElementEquals(expected, aggregatedElements);
    }

    @Test
    public void shouldQueryAggregateElementsWithGroupByAndViewAggregator() {
        // given
        final Schema schema = Schema.fromJson(StreamUtil.openStreams(getClass(), "schema-groupby"));
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .aggregator(new ElementAggregator.Builder()
                                .select("count")
                                .execute(new Product())
                                .build())
                        .build())
                .entity(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();

        final List<Element> elements = Arrays.asList(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 1)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 2)
                        .property("property2", "value1")
                        .property("visibility", "vis2")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 2)
                        .property("property2", "value2")
                        .property("visibility", "vis1")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 10)
                        .property("property2", "value2")
                        .property("visibility", "vis2")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex2")
                        .property("count", 20)
                        .property("property2", "value10")
                        .property("visibility", "vis1")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 100)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 200)
                        .property("property2", "value1")
                        .property("visibility", "vis2")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 1000)
                        .property("property2", "value2")
                        .property("visibility", "vis1")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 2000)
                        .property("property2", "value2")
                        .property("visibility", "vis2")
                        .build());

        final Set<Element> expected = Sets.newHashSet(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 40)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex2")
                        .property("count", 20)
                        .property("property2", "value10")
                        .property("visibility", "vis1")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .property("count", 3300)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build());

        // when
        final Iterable<Element> aggregatedElements = AggregatorUtil.queryAggregate(elements, schema, view);

        // then
        assertElementEquals(expected, aggregatedElements);
    }

    @Test
    public void shouldQueryAggregateElementsWhenProvidedIterableCanOnlyBeConsumedOnce() {
        // given
        final Schema schema = Schema.fromJson(StreamUtil.openStreams(getClass(), "schema-groupby"));
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .entity(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();
        final List<Element> elements = Arrays.asList(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .property("count", 1)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.NON_AGG_ENTITY)
                        .vertex("vertex1")
                        .property("count", 2)
                        .build());

        final Iterable<Element> onlyConsumingOnceIterable = new Iterable<Element>() {
            private boolean firstTime = true;

            @Override
            public Iterator<Element> iterator() {

                if (firstTime) {
                    firstTime = false;
                    return elements.iterator();
                }

                throw new NoSuchElementException();
            }
        };

        // when
        final Iterable<Element> aggregatedElements = AggregatorUtil.queryAggregate(onlyConsumingOnceIterable, schema, view);

        // then
        assertElementEquals(elements, aggregatedElements);
    }

    @Test
    public void shouldCreateIngestElementKeyUsingVertex() {
        // given
        final Schema schema = Schema.fromJson(StreamUtil.openStream(getClass(), "/schema/elements.json"));
        final List<Element> input = Arrays.asList(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex2")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .build());

        // when
        final Function<Element, Element> fn = new AggregatorUtil.ToIngestElementKey(schema);

        // then
        assertThat(fn.apply(input.get(0)))
                .isEqualTo(new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .build());
        assertThat(fn.apply(input.get(1)))
                .isEqualTo(new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex2")
                        .build());
        assertThat(fn.apply(input.get(2)))
                .isEqualTo(new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertex2")
                        .dest("vertex1")
                        .build());

        final Map<Element, List<Element>> results = input.stream().collect(Collectors.groupingBy(fn));
        final Map<Element, List<Element>> expected = new HashMap<>();
        expected.put(input.get(0), Lists.newArrayList(input.get(0)));
        expected.put(input.get(1), Lists.newArrayList(input.get(1)));
        expected.put(input.get(2), Lists.newArrayList(input.get(2)));
        assertThat(results).isEqualTo(expected);
    }

    @Test
    public void shouldCreateIngestElementKeyUsingGroup() {
        // given
        final Schema schema = createSchema();
        final List<Element> input = Arrays.asList(
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY_2)
                        .vertex("vertex1")
                        .build());

        // when
        final AggregatorUtil.ToIngestElementKey fn = new AggregatorUtil.ToIngestElementKey(schema);

        // then
        assertThat(fn.apply(input.get(0)))
                .isEqualTo(new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertex1")
                        .build());
        assertThat(fn.apply(input.get(1)))
                .isEqualTo(new Entity.Builder()
                        .group(TestGroups.ENTITY_2)
                        .vertex("vertex1")
                        .build());
        final Map<Element, List<Element>> results = input.stream()
                .collect(Collectors.groupingBy(fn));
        assertThat(results).hasSize(2);

        assertThat(results.get(input.get(0)).get(0)).isEqualTo(input.get(0));
        assertThat(results.get(input.get(1)).get(0)).isEqualTo(input.get(1));
    }

    @Test
    public void shouldCreateIngestElementKeyUsingGroupByProperties() {
        // given
        final Schema schema = Schema.fromJson(StreamUtil.openStreams(getClass(), "schema-groupby"));

        // when
        final Function<Element, Element> fn = new AggregatorUtil.ToIngestElementKey(schema);

        // then
        assertThat(fn.apply(new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("vertex1")
                .property("property1", "value1")
                .property("property2", "value2")
                .property("property3", "value3")
                .property("visibility", "vis1")
                .build()))
                        .isEqualTo(new Entity.Builder()
                                .group(TestGroups.ENTITY)
                                .vertex("vertex1")
                                .property("property2", "value2")
                                .property("property3", "value3")
                                .property("visibility", "vis1")
                                .build());

        assertThat(fn.apply(new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("vertex1")
                .dest("vertex2")
                .directed(true)
                .property("property1", "value1")
                .property("property2", "value2")
                .property("property3", "value3")
                .property("visibility", "vis1")
                .build()))
                        .isEqualTo(new Edge.Builder()
                                .group(TestGroups.EDGE)
                                .source("vertex1")
                                .dest("vertex2")
                                .directed(true)
                                .property("property2", "value2")
                                .property("property3", "value3")
                                .property("visibility", "vis1")
                                .build());
    }

    @Test
    public void shouldCreateQueryElementKeyUsingViewGroupByProperties() {
        // given
        final Schema schema = Schema.fromJson(StreamUtil.openStreams(getClass(), "schema-groupby"));
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy("property2")
                        .build())
                .entity(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy("property2")
                        .build())
                .build();

        // when
        final Function<Element, Element> fn = new AggregatorUtil.ToQueryElementKey(schema, view);

        // then
        assertThat(fn.apply(new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("vertex1")
                .property("property1", "value1")
                .property("property2", "value2")
                .property("property3", "value3")
                .property("visibility", "vis1")
                .build()))
                        .isEqualTo(new Entity.Builder()
                                .group(TestGroups.ENTITY)
                                .vertex("vertex1")
                                .property("property2", "value2")
                                .build());

        assertThat(fn.apply(new Edge.Builder()
                .group(TestGroups.EDGE)
                .source("vertex1")
                .dest("vertex2")
                .directed(true)
                .property("property1", "value1")
                .property("property2", "value2")
                .property("property3", "value3")
                .property("visibility", "vis1")
                .build()))
                        .isEqualTo(new Edge.Builder()
                                .group(TestGroups.EDGE)
                                .source("vertex1")
                                .dest("vertex2")
                                .directed(true)
                                .property("property2", "value2")
                                .build());
    }

    @Test
    public void shouldThrowExceptionWhenCreateIngestElementKeyIfElementBelongsToGroupThatDoesntExistInSchema() {
        // given
        final Schema schema = createSchema();
        final Element element = new Entity.Builder()
                .group("Unknown group")
                .vertex("vertex1")
                .property("Meaning of life", 42)
                .build();

        // when
        final Function<Element, Element> fn = new AggregatorUtil.ToIngestElementKey(schema);

        // then
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> fn.apply(element)).extracting("message").isNotNull();
    }

    @Test
    public void shouldThrowExceptionWhenCreateQueryElementKeyIfElementBelongsToGroupThatDoesntExistInSchema() {
        // given
        final Schema schema = createSchema();
        final Element element = new Entity.Builder()
                .group("Unknown group")
                .vertex("vertex1")
                .property("Meaning of life", 42)
                .build();

        // when
        final Function<Element, Element> fn = new AggregatorUtil.ToQueryElementKey(schema, new View());

        // then
        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> fn.apply(element)).extracting("message").isNotNull();
    }

    @Test
    public void shouldGroupElementsWithSameIngestElementKey() {
        // given
        final Schema schema = createSchema();

        // when
        final List<Element> input = Arrays.asList(
                new Entity.Builder()
                        .group(TestGroups.ENTITY_2)
                        .vertex("vertex1")
                        .property(TestPropertyNames.PROP_1, "control value")
                        .property(TestPropertyNames.PROP_3, "unused")
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY_2)
                        .vertex("vertex1")
                        .property(TestPropertyNames.PROP_1, "control value")
                        .property(TestPropertyNames.PROP_3, "also unused in function")
                        .build());

        final Map<Element, List<Element>> results =
                input.stream().collect(
                        Collectors.groupingBy(
                                new AggregatorUtil.ToIngestElementKey(schema)));

        // then
        assertThat(results).hasSize(1);
        assertThat(results.get(new Entity.Builder()
                .group(TestGroups.ENTITY_2)
                .vertex("vertex1")
                .property(TestPropertyNames.PROP_1, "control value")
                .build()))
                        .isEqualTo(input);
    }

    private Schema createSchema() {
        return new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                        .property(TestPropertyNames.PROP_2, TestTypes.PROP_INTEGER)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                        .groupBy(TestPropertyNames.PROP_1)
                        .description(SchemaTest.EDGE_DESCRIPTION)
                        .validator(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                        .property(TestPropertyNames.PROP_2, TestTypes.PROP_INTEGER)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                        .groupBy(TestPropertyNames.PROP_1)
                        .description(SchemaTest.EDGE_DESCRIPTION)
                        .validator(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                        .property(TestPropertyNames.PROP_2, TestTypes.PROP_INTEGER)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                        .groupBy(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                        .description(SchemaTest.ENTITY_DESCRIPTION)
                        .validator(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .description(SchemaTest.STRING_TYPE_DESCRIPTION)
                        .build())
                .type(TestTypes.PROP_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .description(SchemaTest.STRING_TYPE_DESCRIPTION)
                        .build())
                .type(TestTypes.PROP_INTEGER, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .description(SchemaTest.INTEGER_TYPE_DESCRIPTION)
                        .build())
                .type(TestTypes.TIMESTAMP, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .description(SchemaTest.TIMESTAMP_TYPE_DESCRIPTION)
                        .build())
                .visibilityProperty(TestPropertyNames.VISIBILITY)
                .build();
    }

    @Test
    public void shouldQueryAggregateDirectedEdgesIncludingMatchedVertexGroupBy() {
        // given
        final Schema schema = Schema.fromJson(StreamUtil.openStreams(getClass(), "schema-groupby"));
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .entity(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();

        final List<Element> elements = createElementsForIncludeMatchedVertexTest();

        final Set<Element> expectedIncludingMatchedVertex = Sets.newHashSet(
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("1-sourceDir3")
                        .dest("2-destDir3")
                        .directed(true)
                        .matchedVertex(EdgeId.MatchedVertex.DESTINATION)
                        .property("count", 21)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("1-sourceDir3")
                        .dest("2-destDir3")
                        .directed(true)
                        .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                        .property("count", 4300)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build());

        // when
        final Iterable<Element> aggregatedElementsIncludingMatchedVertex = AggregatorUtil.queryAggregate(elements, schema, view, true);

        // then
        assertElementEqualsIncludingMatchedVertex(expectedIncludingMatchedVertex, aggregatedElementsIncludingMatchedVertex);
    }

    @Test
    public void shouldQueryAggregateDirectedEdgesExcludingMatchedVertexGroupBy() {
        // given
        final Schema schema = Schema.fromJson(StreamUtil.openStreams(getClass(), "schema-groupby"));
        final View view = new View.Builder()
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .entity(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .groupBy()
                        .build())
                .build();

        final List<Element> elements = createElementsForIncludeMatchedVertexTest();

        final Set<Element> expectedExcludingMatchedVertex = Sets.newHashSet(
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("1-sourceDir3")
                        .dest("2-destDir3")
                        .directed(true)
                        .property("count", 4321)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build());

        // when
        final Iterable<Element> aggregatedElementsExcludingMatchedVertex = AggregatorUtil.queryAggregate(elements, schema, view, false);

        // then
        assertElementEquals(expectedExcludingMatchedVertex, aggregatedElementsExcludingMatchedVertex);
    }

    private List<Element> createElementsForIncludeMatchedVertexTest() {
        return Arrays.asList(
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("1-sourceDir3")
                        .dest("2-destDir3")
                        .directed(true)
                        .matchedVertex(EdgeId.MatchedVertex.DESTINATION)
                        .property("count", 1)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("1-sourceDir3")
                        .dest("2-destDir3")
                        .directed(true)
                        .matchedVertex(EdgeId.MatchedVertex.DESTINATION)
                        .property("count", 20)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("1-sourceDir3")
                        .dest("2-destDir3")
                        .directed(true)
                        .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                        .property("count", 300)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("1-sourceDir3")
                        .dest("2-destDir3")
                        .directed(true)
                        .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                        .property("count", 4000)
                        .property("property2", "value1")
                        .property("visibility", "vis1")
                        .build());
    }
}
