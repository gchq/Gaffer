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

package uk.gov.gchq.koryphe.function;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Applies a {@link Function} to the values of a {@link Map}.
 *
 * @param <I> Type of input value
 * @param <O> Type of output value
 */
public class FunctionMap<K, I, O> implements Function<Map<K, I>, Map<K, O>> {
    private Function<I, O> function;

    public FunctionMap() {
    }

    public FunctionMap(final Function<I, O> function) {
        setFunction(function);
    }

    public void setFunction(final Function<I, O> function) {
        this.function = function;
    }

    public Function<I, O> getFunction() {
        return function;
    }

    @Override
    public Map<K, O> apply(final Map<K, I> input) {
        if (input == null) {
            return null;
        } else {
            Map<K, O> transformed = new HashMap<>(input.size());
            for (Map.Entry<K, I> entry : input.entrySet()) {
                transformed.put(entry.getKey(), function.apply(entry.getValue()));
            }
            return transformed;
        }
    }
}
