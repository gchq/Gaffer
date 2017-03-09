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

package koryphe.function.aggregate;

import koryphe.function.combine.CombineByKey;

import java.util.Map;

/**
 * Applies an {@link Aggregator} to the values of an input {@link Map}, updating the output {@link Map} with the current
 * state.
 * @param <K> Type of key
 * @param <T> Type of input/output value
 */
public class AggregateByKey<K, T> extends CombineByKey<K, T, T> implements Aggregator<Map<K, T>> {
    public AggregateByKey() { }

    public AggregateByKey(final Aggregator<T> function) {
        setFunction(function);
    }
}
