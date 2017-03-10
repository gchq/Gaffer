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

package uk.gov.gchq.koryphe.binaryoperator;

import uk.gov.gchq.koryphe.bifunction.BiFunctionMap;
import java.util.Map;
import java.util.function.BinaryOperator;

/**
 * Applies an {@link BinaryOperator} to the values of an input {@link Map}, updating the output {@link Map} with the current
 * state.
 *
 * @param <K> Type of key
 * @param <T> Type of input/output value
 */
public class BinaryOperatorMap<K, T> extends BiFunctionMap<K, T, T> implements BinaryOperator<Map<K, T>> {
    public BinaryOperatorMap() {
    }

    public BinaryOperatorMap(final BinaryOperator<T> function) {
        setFunction(function);
    }
}
