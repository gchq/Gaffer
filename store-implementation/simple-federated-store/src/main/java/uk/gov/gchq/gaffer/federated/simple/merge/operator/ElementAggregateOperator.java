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

package uk.gov.gchq.gaffer.federated.simple.merge.operator;

import org.apache.commons.collections4.IterableUtils;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.HashSet;
import java.util.Set;
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
        // Just append the state and update so we can loop over it to do accurate merging
        // We can't use the original iterators directly in case they close or become exhausted so save to a Set first.
        Set<Element> chainedResult = new HashSet<>(IterableUtils.toList(IterableUtils.chainedIterable(update, state)));

        // Iterate over the chained result to merge the elements with each other
        // Collect to a set to ensure deduplication
        return chainedResult.stream()
            .map(e -> {
                Element result = e;
                // Set up the aggregator for this group based on the schema
                ElementAggregator aggregator = new ElementAggregator();
                if (schema != null) {
                    aggregator = schema.getElement(e.getGroup()).getIngestAggregator();
                }
                // Compare the current element with all others to do a full merge
                for (final Element inner : chainedResult) {
                    // No merge required if not in same group
                    if (!e.getGroup().equals(inner.getGroup()) || e.equals(inner)) {
                        continue;
                    }

                    if ((e instanceof Entity)
                            && (inner instanceof Entity)
                            && ((Entity) e).getVertex().equals(((Entity) inner).getVertex())) {
                        result = aggregator.apply(inner.shallowClone(), result).shallowClone();
                    }

                    if ((e instanceof Edge)
                            && (inner instanceof Edge)
                            && ((Edge) e).getSource().equals(((Edge) inner).getSource())
                            && ((Edge) e).getDestination().equals(((Edge) inner).getDestination())
                            && ((Edge) e).getDirectedType().equals(((Edge) inner).getDirectedType())) {
                        result = aggregator.apply(inner.shallowClone(), result);
                    }
                }
                return result;
            })
            .collect(Collectors.toSet());
    }

}
