/*
 * Copyright 2017 Crown Copyright
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

import com.google.common.collect.Iterables;
import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.GroupedProperties;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import java.util.Collection;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Utility methods to help with doing aggregation of elements. Aggregation differs
 * depending on if it ingest or query time aggregation. Ingest aggregation uses
 * the groupBy properties in a {@link Schema}, whereas query time aggregation first
 * checks the {@link View} to see if the groupBy properties have been overridden.
 */
public final class AggregatorUtil {
    private AggregatorUtil() {
    }

    /**
     * Applies ingest aggregation to the provided iterable of {@link Element}s.
     * This uses the groupBy properties in the provided {@link Schema} to group
     * the elements prior to aggregating them.
     * <p>
     * NOTE - this is done in memory so the size of the iterable should be limited.
     *
     * @param elements the elements to be aggregated
     * @param schema   the schema containing the aggregators and groupBy properties to use
     * @return the aggregated elements.
     */
    public static CloseableIterable<Element> ingestAggregate(final Iterable<? extends Element> elements, final Schema schema) {
        if (null == schema) {
            throw new IllegalArgumentException("Schema is required");
        }
        final Collection<String> aggregatedGroups = schema.getAggregatedGroups();
        final Iterable<? extends Element> nonAggregatedElements = Iterables.filter(elements, new IsElementNotAggregatedGPredicate(aggregatedGroups));
        final Iterable<? extends Element> aggregatableElements = Iterables.filter(elements, new IsElementAggregatedGPredicate(aggregatedGroups));
        final Iterable<Element> aggregatedElements = Streams.toStream(aggregatableElements)
                .collect(Collectors.groupingBy(new ToIngestElementKey(schema), Collectors.reducing(null, new IngestElementBinaryOperator(schema))))
                .values();
        return new ChainedIterable<>(aggregatedElements, nonAggregatedElements);
    }

    /**
     * Applies query time aggregation to the provided iterable of {@link Element}s.
     * This uses the groupBy properties in the provided {@link View} or {@link Schema} to group
     * the elements prior to aggregating them.
     * <p>
     * NOTE - this is done in memory so the size of the iterable should be limited.
     *
     * @param elements the elements to be aggregated
     * @param schema   the schema containing the aggregators and groupBy properties to use
     * @param view     the view containing the aggregators and groupBy properties to use
     * @return the aggregated elements.
     */
    public static CloseableIterable<Element> queryAggregate(final Iterable<? extends Element> elements, final Schema schema, final View view) {
        if (null == schema) {
            throw new IllegalArgumentException("Schema is required");
        }
        if (null == view) {
            throw new IllegalArgumentException("View is required");
        }
        final Collection<String> aggregatedGroups = schema.getAggregatedGroups();
        final Iterable<? extends Element> nonAggregatedElements = Iterables.filter(elements, new IsElementNotAggregatedGPredicate(aggregatedGroups));
        final Iterable<? extends Element> aggregatableElements = Iterables.filter(elements, new IsElementAggregatedGPredicate(aggregatedGroups));
        final Iterable<Element> aggregatedElements = Streams.toStream(aggregatableElements)
                .collect(Collectors.groupingBy(new ToQueryElementKey(schema, view), Collectors.reducing(null, new QueryElementBinaryOperator(schema, view))))
                .values();
        return new ChainedIterable<>(aggregatedElements, nonAggregatedElements);
    }

    /**
     * A Function that takes and element as input and outputs an element key that consists of
     * the Group-by values in the {@link Schema}, the Identifiers and the Group. These act as a key and can be used in a
     * Collector to do ingest aggregation.
     */
    public static class ToIngestElementKey implements Function<Element, Element> {
        private final Schema schema;

        public ToIngestElementKey(final Schema schema) {
            if (null == schema) {
                throw new IllegalArgumentException("Schema is required");
            }
            this.schema = schema;
        }

        @Override
        public Element apply(final Element element) {
            final Element key = element.emptyClone();
            for (final String propertyName : getGroupBy(element, schema)) {
                key.putProperty(propertyName, element.getProperty(propertyName));
            }
            return key;
        }
    }

    /**
     * A Function that takes and element as input and outputs an element key that consists of
     * the Group-by values in the {@link View}, the Identifiers and the Group. These act as a key and can be used in a
     * Collector to do query aggregation.
     */
    public static class ToQueryElementKey implements Function<Element, Element> {
        private final Schema schema;
        private final View view;

        public ToQueryElementKey(final Schema schema, final View view) {
            if (null == schema) {
                throw new IllegalArgumentException("Schema is required");
            }
            if (null == view) {
                throw new IllegalArgumentException("View is required");
            }
            this.schema = schema;
            this.view = view;
        }

        @Override
        public Element apply(final Element element) {
            final Element key = element.emptyClone();
            for (final String propertyName : getGroupBy(element, schema, view)) {
                key.putProperty(propertyName, element.getProperty(propertyName));
            }
            return key;
        }
    }

    public static class IngestElementBinaryOperator implements BinaryOperator<Element> {
        private final Schema schema;

        public IngestElementBinaryOperator(final Schema schema) {
            if (null == schema) {
                throw new IllegalArgumentException("Schema is required");
            }
            this.schema = schema;
        }

        @Override
        public Element apply(final Element a, final Element b) {
            if (null == a) {
                return b;
            }
            if (null == b) {
                return a;
            }
            return schema.getElement(a.getGroup()).getIngestAggregator().apply(a, b);
        }
    }

    public static class QueryElementBinaryOperator implements BinaryOperator<Element> {
        private final Schema schema;
        private final View view;

        public QueryElementBinaryOperator(final Schema schema, final View view) {
            if (null == schema) {
                throw new IllegalArgumentException("Schema is required");
            }
            if (null == view) {
                throw new IllegalArgumentException("View is required");
            }
            this.view = view;
            this.schema = schema;
        }

        @Override
        public Element apply(final Element a, final Element b) {
            if (null == a) {
                return b;
            }
            if (null == b) {
                return a;
            }
            final String group = a.getGroup();
            return schema.getElement(group).getQueryAggregator(view.getElement(group).getGroupBy()).apply(a, b);
        }
    }

    public static class IngestPropertiesBinaryOperator implements BinaryOperator<GroupedProperties> {
        private final Schema schema;

        public IngestPropertiesBinaryOperator(final Schema schema) {
            if (null == schema) {
                throw new IllegalArgumentException("Schema is required");
            }
            this.schema = schema;
        }

        @Override
        public GroupedProperties apply(final GroupedProperties a, final GroupedProperties b) {
            if (null == a) {
                return b;
            }
            if (null == b) {
                return a;
            }
            schema.getElement(a.getGroup()).getIngestAggregator().apply(a, b);
            // The aggregator will always return a so this is safe
            return a;
        }
    }

    public static class QueryPropertiesBinaryOperator implements BinaryOperator<GroupedProperties> {
        private final Schema schema;
        private final View view;

        public QueryPropertiesBinaryOperator(final Schema schema, final View view) {
            if (null == schema) {
                throw new IllegalArgumentException("Schema is required");
            }
            if (null == view) {
                throw new IllegalArgumentException("View is required");
            }
            this.schema = schema;
            this.view = view;
        }

        @Override
        public GroupedProperties apply(final GroupedProperties a, final GroupedProperties b) {
            if (null == a) {
                return b;
            }
            if (null == b) {
                return a;
            }
            final String group = a.getGroup();
            schema.getElement(group).getQueryAggregator(view.getElement(group).getGroupBy()).apply(a, b);
            // The aggregator will always return a so this is safe
            return a;
        }
    }

    public static class IsElementAggregated implements Predicate<Element> {
        final Collection<String> aggregatedGroups;

        public IsElementAggregated(final Schema schema) {
            if (null == schema) {
                throw new IllegalArgumentException("Schema is required");
            }
            this.aggregatedGroups = schema.getAggregatedGroups();
        }

        public IsElementAggregated(final Collection<String> aggregatedGroups) {
            if (null == aggregatedGroups) {
                throw new IllegalArgumentException("Aggregated groups is required");
            }
            this.aggregatedGroups = aggregatedGroups;
        }

        @Override
        public boolean test(final Element element) {
            return null != element && aggregatedGroups.contains(element.getGroup());
        }
    }

    private static final class IsElementAggregatedGPredicate implements com.google.common.base.Predicate<Element> {
        final Collection<String> aggregatedGroups;

        private IsElementAggregatedGPredicate(final Collection<String> aggregatedGroups) {
            if (null == aggregatedGroups) {
                throw new IllegalArgumentException("Aggregated groups is required");
            }
            this.aggregatedGroups = aggregatedGroups;
        }

        @Override
        public boolean apply(final Element element) {
            return null != element && aggregatedGroups.contains(element.getGroup());
        }
    }

    private static final class IsElementNotAggregatedGPredicate implements com.google.common.base.Predicate<Element> {
        final Collection<String> aggregatedGroups;

        private IsElementNotAggregatedGPredicate(final Collection<String> aggregatedGroups) {
            if (null == aggregatedGroups) {
                throw new IllegalArgumentException("Aggregated groups is required");
            }
            this.aggregatedGroups = aggregatedGroups;
        }

        @Override
        public boolean apply(final Element element) {
            return null != element && !aggregatedGroups.contains(element.getGroup());
        }
    }

    private static Set<String> getGroupBy(final Element element, final Schema schema) {
        final SchemaElementDefinition elDef = schema.getElement(element.getGroup());
        if (elDef == null) {
            throw new IllegalArgumentException("Received element " + element
                    + " which belongs to a group not found in the schema");
        }
        return elDef.getGroupBy();
    }

    private static Set<String> getGroupBy(final Element element, final Schema schema, final View view) {
        final String group = element.getGroup();
        Set<String> groupBy = null;
        if (null != view) {
            final ViewElementDefinition elDef = view.getElement(group);
            if (null != elDef) {
                groupBy = elDef.getGroupBy();
            }
        }
        if (null == groupBy) {
            final SchemaElementDefinition elDef = schema.getElement(group);
            if (elDef == null) {
                throw new IllegalArgumentException("Received element " + element
                        + " which belongs to a group not found in the schema");
            }
            groupBy = elDef.getGroupBy();
        }
        return groupBy;
    }
}
