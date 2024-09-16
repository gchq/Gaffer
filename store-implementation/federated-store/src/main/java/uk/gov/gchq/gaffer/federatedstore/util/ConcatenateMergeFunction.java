/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.util;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.koryphe.iterable.ChainedIterable;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.BiFunction;

import static java.util.Objects.nonNull;

/**
 * A BiFunction for concatenating results from multiple graphs into one Iterable.
 */
public class ConcatenateMergeFunction implements BiFunction<Object, Iterable<Object>, Iterable<Object>> {

    /**
     * Concatenates the result from a graph with the previous results Iterable.
     *
     * @param update    the result to be added
     * @param state     the iterable of previous results
     */
    @Override
    public Iterable<Object> apply(final Object update, final Iterable<Object> state) {
        final Iterable<Object> updateSafe;
        // When update is null, don't add an Iterable containing a null.
        if (update == null) {
            updateSafe = Collections.emptyList();
        } else if (!(update instanceof Iterable)) {
            // When update is an Array, convert it to a List; otherwise wrap it in a List.
            updateSafe = (update instanceof Object[]) ? Arrays.asList((Object[]) update) : Collections.singletonList(update);
        } else {
            updateSafe = (Iterable<Object>) update;
        }
        // If current state is null, return the update which will be an Iterable.
        // Otherwise, chain the update with the previous state
        return (state == null) ? updateSafe : new ChainedIterable<>(updateSafe, state);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(13, 47)
                .append(super.hashCode())
                .toHashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return nonNull(obj) && this.getClass().equals(obj.getClass());
    }
}
