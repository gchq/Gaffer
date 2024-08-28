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

import java.util.Iterator;
import java.util.function.BinaryOperator;
import java.util.stream.StreamSupport;

import org.apache.commons.collections4.IterableUtils;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.store.schema.Schema;

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
    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    @Override
    public Iterable<Element> apply(Iterable<Element> update, Iterable<Element> state) {
        // Just append the state and update so we can loop over it to do accurate merging
        Iterable<Element> chainedMerge = IterableUtils.chainedIterable(update, state);

        // Custom merge iterable for lazy processing
        Iterable<Element> mergeIterable = () ->
            new Iterator<Element>() {
                // An iterator over the chained state and update iterables gives an accurate hasNext
                Iterator<Element> chainedMergeIterator = chainedMerge.iterator();

                @Override
                public boolean hasNext() {
                    return chainedMergeIterator.hasNext();
                }

                @Override
                public Element next() {
                    // When requested do element aggregation on for the current element if required
                    Element current = chainedMergeIterator.next();
                    Element result = current;

                    // Set up the aggregator for this group based on the schema
                    ElementAggregator aggregator = new ElementAggregator();
                    if (schema != null) {
                        aggregator= schema.getElement(current.getGroup()).getIngestAggregator();
                    }

                    // Compare the current element with all others to do a full merge
                    for (Element inner : chainedMerge) {
                        // No merge required if not in same group
                        if (!current.getGroup().equals(inner.getGroup())) {
                            continue;
                        }

                        if ((current instanceof Entity)
                                && (inner instanceof Entity)
                                && ((Entity) current).getVertex().equals(((Entity) inner).getVertex())) {
                            result = aggregator.apply(inner, result);

                        }

                        if ((current instanceof Edge)
                                && (inner instanceof Edge)
                                && ((Edge) current).getSource().equals(((Edge) inner).getSource())
                                && ((Edge) current).getDestination().equals(((Edge) inner).getDestination())) {
                            result = aggregator.apply(inner, result);
                        }
                    }
                    return result;
                }
            };

        // Use stream to dedupe the merged result
        return () -> StreamSupport.stream(mergeIterable.spliterator(), false)
                .distinct()
                .iterator();
    }

}
