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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

/**
 * Operator for aggregating two iterables of {@link Element}s together, this
 * will ensure all properties for similar elements are merged using the
 * {@link ElementAggregator} from the schema to perform the actual aggregation.
 */
public class ElementAggregateOperator implements BinaryOperator<Iterable<Element>> {

    // The schema to use for pulling aggregation functions from
    private Schema schema;

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
        List<Element> updateList = IterableUtils.toList(update);
        List<Element> stateList = IterableUtils.toList(state);

        // Group the elements into lists
        final Map<String, List<Element>> groupedElements = updateList
                .stream()
                .collect(Collectors.groupingBy(this::getElementKey));

        stateList.forEach(e -> groupedElements.computeIfAbsent(getElementKey(e), k -> new ArrayList<>()).add(e));

        // If the elements for a group should be aggregated, do so
        // Otherwise keep all the elements
        List<Element> merged = new ArrayList<>();
        for (List<Element> elementsInGroup : groupedElements.values()) {
            if (elementsInGroup.size() <= 1) {
                merged.addAll(elementsInGroup);
                continue;
            }

            // Merge Elements in these smaller lists
            final String group = elementsInGroup.get(0).getGroup();
            final ElementAggregator aggregator;
            boolean shouldMergeGroup = false;
            if (schema != null) {
                final SchemaElementDefinition elementDefinition = schema.getElement(group);
                aggregator = elementDefinition.getIngestAggregator();
                shouldMergeGroup = elementDefinition.isAggregate();
            } else {
                aggregator = new ElementAggregator();
            }

            if (shouldMergeGroup) {
                Element m = null;
                for (Element e : elementsInGroup) {
                    m = aggregator.apply(m, e);
                }
                merged.add(m);
            } else {
                merged.addAll(elementsInGroup);
            }
        }
        return merged;
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
