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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class OtherElementAggregateOperator extends ElementAggregateOperator {

    @Override
    public Iterable<Element> apply(final Iterable<Element> update, final Iterable<Element> state) {
        final List<Element> stateList = IterableUtils.toList(state);
        final List<Element> updateList = IterableUtils.toList(update);

        for (final Element element : stateList) {
            // Set up the aggregator for this group based on the schema
            ElementAggregator aggregator = new ElementAggregator();
            boolean shouldMergeGroup = false;
            if (schema != null) {
                final SchemaElementDefinition elementDefinition = schema.getElement(element.getGroup());
                aggregator = elementDefinition.getIngestAggregator();
                shouldMergeGroup = elementDefinition.isAggregate();
            }

            if (!shouldMergeGroup) {
                continue;
            }

            final Collection<Element> deleted = new ArrayList<>();
            for (final Element otherElement : updateList) {
                if (!element.getGroup().equals(otherElement.getGroup())) {
                    continue;
                }

                if (element.equals(otherElement)) {
                    deleted.add(otherElement);
                } else if (canMerge(element, otherElement)) {
                    // Mutates element
                    aggregator.apply(element, otherElement);
                    deleted.add(otherElement);
                }
            }

            updateList.removeAll(deleted);
        }

        // Add anything left in the other List
        // These must be elements that aren't in the first list
        stateList.addAll(updateList);
        return stateList;
    }

    private boolean canMerge(final Element element, final Element otherElement) {
        return canMergeEdge(element, otherElement) || canMergeEntity(element, otherElement);
    }

    private boolean canMergeEntity(final Element element, final Element otherElement) {
        return (element instanceof Entity)
                && (otherElement instanceof Entity)
                && ((Entity) element).getVertex().equals(((Entity) otherElement).getVertex());
    }

    private boolean canMergeEdge(final Element element, final Element otherElement) {
        return (element instanceof Edge)
                && (otherElement instanceof Edge)
                && ((Edge) element).getSource().equals(((Edge) otherElement).getSource())
                && ((Edge) element).getDestination().equals(((Edge) otherElement).getDestination())
                && ((Edge) element).getDirectedType().equals(((Edge) otherElement).getDirectedType());
    }

}
