/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.time.binaryoperator;

import uk.gov.gchq.gaffer.time.BoundedTimestampSet;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

/**
 * A {@code BoundedTimestampSetAggregator} is a {@link java.util.function.BinaryOperator} that takes in
 * {@link BoundedTimestampSet}s and aggregates the second one into the first. The {@link BoundedTimestampSet}s can
 * only be aggregated if they have the same time bucket and maximum size.
 */
@Since("1.0.0")
@Summary("Aggregates BoundedTimestampSets")
public class BoundedTimestampSetAggregator extends KorypheBinaryOperator<BoundedTimestampSet> {

    @Override
    protected BoundedTimestampSet _apply(final BoundedTimestampSet a, final BoundedTimestampSet b) {
        // Can only be merged if they have the same time bucket and maximum size.
        if (!a.getTimeBucket().equals(b.getTimeBucket())) {
            throw new IllegalArgumentException("Can only merge two BoundedTimestampSet with the same time bucket:" +
                    "a has time bucket " + a.getTimeBucket() + ", b has time bucket " + b.getTimeBucket());
        }
        if (a.getMaxSize() != b.getMaxSize()) {
            throw new IllegalArgumentException("Can only merge two BoundedTimestampSet with the same maximum size:" +
                    "a has maximum size " + a.getMaxSize() + ", b has maximum size " + b.getMaxSize());
        }
        // If both are in NOT_FULL state then all the timestamps for b can just be added to a.
        if (a.getState().equals(BoundedTimestampSet.State.NOT_FULL)
                && b.getState().equals(BoundedTimestampSet.State.NOT_FULL)) {
            a.add(b.getTimestamps());
            return a;
        }
        // If either a or b or both are in SAMPLE state, then the aggregated version must also be in SAMPLE state.
        if (a.getState().equals(BoundedTimestampSet.State.NOT_FULL)) {
            // Need to switch the state of a to SAMPLE and add in b's samples.
            a.switchToSampleState();
            a.add(b.getTimestamps());
        } else {
            a.add(b.getTimestamps());
        }
        return a;
    }
}
