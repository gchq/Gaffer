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

import org.apache.commons.collections4.IterableUtils;

import uk.gov.gchq.gaffer.data.element.Element;

import java.util.Collection;

/**
 * The default result accumulator for merging results from multiple graphs into one.
 */
public class DefaultResultAccumulator<T> extends FederatedResultAccumulator<T> {

    /**
     * Concatenates the result from a graph with the previous result.
     *
     * @param update    the result to be added
     * @param state     the previous results
     */
    @SuppressWarnings("unchecked")
    @Override
    public T apply(final T update, final T state) {
        // Don't update if null
        if (update == null) {
            return state;
        }

        // Use configured number merger
        if (state instanceof Number) {
            return (T) this.numberMergeOperator.apply((Number) update, (Number) state);
        }

        // Use configured string merger
        if (state instanceof String) {
            return (T) this.stringMergeOperator.apply((String) update, (String) state);
        }

        // Use configured boolean merger
        if (state instanceof Boolean) {
            return (T) this.booleanMergeOperator.apply((Boolean) update, (Boolean) state);
        }

        // Use configured collection merger
        if (state instanceof Collection<?>) {
            return (T) this.collectionMergeOperator.apply((Collection<Object>) update, (Collection<Object>) state);
        }

        // If an iterable try merge them
        if (update instanceof Iterable<?>) {
            Iterable<?> updateIterable = (Iterable<?>) update;
            // If nothing to update then exit
            if (!updateIterable.iterator().hasNext()) {
                return state;
            }

            // Should we use the element aggregator operation
            if ((this.aggregateElements) && (updateIterable.iterator().next() instanceof Element))  {
                return (T) this.elementAggregateOperator.apply((Iterable<Element>) update, (Iterable<Element>) state);
            }

            // Default just chain iterables together
            return (T) IterableUtils.chainedIterable((Iterable<?>) state, updateIterable);
        }

        return update;
    }
}
