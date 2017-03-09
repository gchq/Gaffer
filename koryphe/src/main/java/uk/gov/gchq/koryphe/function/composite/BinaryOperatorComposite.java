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

package uk.gov.gchq.koryphe.function.composite;

import java.util.function.BinaryOperator;

/**
 * A composite {@link java.util.function.BinaryOperator} that applies each aggregator in turn, supplying the result of each aggregator as
 * the state of the next, and returning the result of the last aggregator.
 *
 * @param <T> Type of aggregator input/output
 */
public final class BinaryOperatorComposite<T> extends Composite<BinaryOperator<T>> implements BinaryOperator<T> {
    @Override
    public T apply(final T input, final T state) {
        T result = state;
        for (BinaryOperator<T> function : this) {
            result = function.apply(input, result);
        }
        return result;
    }
}
