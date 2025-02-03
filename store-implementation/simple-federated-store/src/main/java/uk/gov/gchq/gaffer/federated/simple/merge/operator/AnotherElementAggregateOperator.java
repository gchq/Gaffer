/*
 * Copyright 2025 Crown Copyright
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
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AnotherElementAggregateOperator extends ElementAggregateOperator {

    @Override
    public Iterable<Element> apply(final Iterable<Element> update, final Iterable<Element> state) {

        List<Element> chained = IterableUtils.toList(IterableUtils.chainedIterable(update, state));

        // Group the elements into lists
        final Map<String, Set<Element>> groupedElements = chained
                .parallelStream()
                .collect(Collectors.toConcurrentMap(
                        this::getIdString,
                        e -> new HashSet<>(Collections.singleton(e)),
                        (existing, replacement) -> {
                            existing.addAll(replacement);
                            return existing;
                        }));

        // If the elements for a group should be aggregated, do so
        // Otherwise keep all the elements
        return groupedElements.values().parallelStream()
                .map(elements -> {
                    if (elements.size() <= 1) {
                        return elements;
                    }

                    Element e = elements.stream().findAny().get();
                    final ElementAggregator aggregator;
                    boolean shouldMergeGroup = false;
                    if (schema != null) {
                        final SchemaElementDefinition elementDefinition = schema
                                .getElement(e.getGroup());
                        aggregator = elementDefinition.getIngestAggregator();
                        shouldMergeGroup = elementDefinition.isAggregate();
                    } else {
                        aggregator = new ElementAggregator();
                    }

                    if (shouldMergeGroup) {
                        return Collections.singletonList(elements.stream()
                                .reduce((a, b) -> aggregator.apply(a.shallowClone(), b.shallowClone())).get());
                    }

                    return elements;
                })
                .flatMap(Collection::stream) // Flatten list of lists
                .collect(Collectors.toList());
    }

    // So we can group Entities and Edges that are the same but with different
    // properties
    private String getIdString(final Element e) {
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
