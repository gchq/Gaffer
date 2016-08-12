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

import java.util.Map;

public class MapAggregator<K, I, O> implements Aggregator<Map<K, I>, Map<K, O>> {
    private Aggregator<I, O> aggregator;

    public MapAggregator() { }

    public MapAggregator(final Aggregator<I, O> aggregator) {
        setAggregator(aggregator);
    }

    public void setAggregator(final Aggregator<I, O> aggregator) {
        this.aggregator = aggregator;
    }

    public Aggregator<I, O> getAggregator() {
        return aggregator;
    }

    @Override
    public Map<K, O> execute(final Map<K, I> input, final Map<K, O> state) {
        if (input == null) {
            return state;
        } else {
            if (state == null) {
                // reuse input map
                Map<K, O> output = (Map<K, O>) input;
                for (Map.Entry<K, I> entry : input.entrySet()) {
                    output.put(entry.getKey(), aggregator.execute(entry.getValue(), null));
                }
                return (Map<K, O>) input;
            } else {
                for (Map.Entry<K, I> entry : input.entrySet()) {
                    O currentState = state.get(entry.getKey());
                    state.put(entry.getKey(), aggregator.execute(entry.getValue(), currentState));
                }
                return state;
            }
        }
    }

    @Override
    public MapAggregator<K, I, O> copy() {
        MapAggregator<K, I, O> copy = new MapAggregator<>();
        copy.setAggregator(aggregator.copy());
        return copy;
    }
}
