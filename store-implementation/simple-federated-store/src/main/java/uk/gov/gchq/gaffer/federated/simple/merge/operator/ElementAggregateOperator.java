/*
 * Copyright 2024-2025 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple.merge.operator;

import org.apache.commons.collections4.IterableUtils;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Operator for aggregating two iterables of {@link Element}s together, this
 * will ensure all properties for similar elements are merged using the
 * {@link ElementAggregator} from the schema to perform the actual aggregation.
 */
public class ElementAggregateOperator implements BinaryOperator<Iterable<Element>> {

    // The schema to use for pulling aggregation functions from
    protected Schema schema;

    /**
     * Set the schema to use for aggregating elements of the same group
     *
     * @param schema The schema.
     */
    public void setSchema(final Schema schema) {
        this.schema = schema;
    }

    @Override
    public Iterable<Element> apply(final Iterable<Element> update, final Iterable<Element> state) {
        // Just append the state and update so we can loop over it to do accurate
        // merging
        // We can't use the original iterators directly in case they close or become
        // exhausted so save to a List first.
        final List<Element> chained = IterableUtils.toList(IterableUtils.chainedIterable(update, state));

        // Group the elements into lists
        final Map<String, List<Element>> groupedElements = chained
                .stream()
                .collect(Collectors.groupingBy(this::getElementKey));

        // If the elements for a group should be aggregated, do so
        // Otherwise keep all the elements
        return groupedElements.values().parallelStream()
                .map(elements -> {
                    // No merging needed
                    if (elements.size() <= 1) {
                        return elements;
                    }

                    // Merge Elements in these smaller lists
                    final String group = elements.get(0).getGroup();
                    final ElementAggregator aggregator;
                    boolean shouldMergeGroup = false;
                    if (schema != null) {
                        final SchemaElementDefinition elementDefinition = schema.getElement(group);
                        aggregator = elementDefinition.getIngestAggregator();
                        shouldMergeGroup = elementDefinition.isAggregate();
                    } else {
                        aggregator = new ElementAggregator();
                    }

                    // dedup
                    final Stream<Element> stream = elements.stream().distinct();

                    if (shouldMergeGroup) {
                        return Collections.singletonList(stream.reduce(aggregator::apply).get());
                    }

                    return stream.collect(Collectors.toList());
                })
                .flatMap(Collection::stream) // Flatten list of lists
                .collect(Collectors.toList());
    }

    // So we can group Elements that are the same but with different properties
    private String getElementKey(final Element e) {
        final StringBuilder builder = new StringBuilder();
        builder.append(e.getGroup());

        if (e instanceof Entity) {
            builder.append(((Entity) e).getVertex().toString());
        } else if (e instanceof Edge) {
            builder.append(((Edge) e).getSource().toString());
            builder.append(((Edge) e).getDestination().toString());
            builder.append(((Edge) e).getDirectedType().toString());
        }

        return builder.toString();
    }

}
