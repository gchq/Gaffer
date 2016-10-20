/*
 * Copyright 2016 Crown Copyright
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

package koryphe.function.stateful.aggregator;

import koryphe.function.MultiFunction;

/**
 * An {@link Aggregator} that applies a list of Aggregators. The input value is given to each aggregator in turn - the
 * state is updated by the aggregator and delivered to the next aggregator in the list alongside the input value.
 * @param <I> Input type
 * @param <O> Output type
 */
public final class MultiAggregator<I, O> extends MultiFunction<Aggregator> implements Aggregator<I, O> {
    @Override
    public O execute(final I input, final O state) {
        Object result = state;
        for (Aggregator aggregator : functions) {
            result = aggregator.execute(input, result);
        }
        return (O) result;
    }
}
