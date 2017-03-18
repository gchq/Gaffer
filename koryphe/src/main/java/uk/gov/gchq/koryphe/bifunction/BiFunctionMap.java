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

package uk.gov.gchq.koryphe.bifunction;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Applies a {@link BiFunction} to the values of an input {@link Map}, updating the output {@link Map} with the current
 * state.
 *
 * @param <K> Type of key
 * @param <I> Type of input value
 * @param <O> Type of output value
 */
public class BiFunctionMap<K, I, O> implements BiFunction<Map<K, I>, Map<K, O>, Map<K, O>> {
    private BiFunction<I, O, O> function;

    public BiFunctionMap() {
    }

    public BiFunctionMap(final BiFunction<I, O, O> function) {
        setFunction(function);
    }

    public void setFunction(final BiFunction<I, O, O> function) {
        this.function = function;
    }

    public BiFunction<I, O, O> getFunction() {
        return function;
    }

    @Override
    public Map<K, O> apply(final Map<K, I> input, final Map<K, O> state) {
        if (input == null) {
            return state;
        } else {
            Map<K, O> output = state == null ? new HashMap<>() : state;
            for (final Map.Entry<K, I> entry : input.entrySet()) {
                O currentState = output.get(entry.getKey());
                output.put(entry.getKey(), function.apply(entry.getValue(), currentState));
            }
            return output;
        }
    }
}
