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

package koryphe.function.combine;

import java.util.HashMap;
import java.util.Map;

/**
 * Applies a {@link Combiner} to the values of an input {@link Map}, updating the output {@link Map} with the current
 * state.
 * @param <K> Type of key
 * @param <I> Type of input value
 * @param <O> Type of output value
 */
public class CombineByKey<K, I, O> implements Combiner<Map<K, I>, Map<K, O>> {
    private Combiner<I, O> function;

    public CombineByKey() { }

    public CombineByKey(final Combiner<I, O> function) {
        setFunction(function);
    }

    public void setFunction(final Combiner<I, O> function) {
        this.function = function;
    }

    public Combiner<I, O> getFunction() {
        return function;
    }

    @Override
    public Map<K, O> execute(final Map<K, I> input, final Map<K, O> state) {
        if (input == null) {
            return state;
        } else {
            Map<K, O> output = state == null ? new HashMap<K, O>() : state;
            for (Map.Entry<K, I> entry : input.entrySet()) {
                O currentState = output.get(entry.getKey());
                output.put(entry.getKey(), function.execute(entry.getValue(), currentState));
            }
            return output;
        }
    }
}
