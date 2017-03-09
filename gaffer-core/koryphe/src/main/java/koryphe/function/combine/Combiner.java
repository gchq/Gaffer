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

import koryphe.function.Function;

/**
 * A <code>Combiner</code> is a {@link Function} that updates state in response to each input, and outputs the current
 * state. If the order in which input values are provided cannot be guaranteed, <code>Combiner</code>s should be
 * commutative and associative to ensure consistent behaviour.
 * @param <I> Function input type
 * @param <O> Function output type
 */
public interface Combiner<I, O> extends Function<I, O> {
    /**
     * Combine next value into current state.
     * @param input New input value.
     * @param state Current state.
     * @return Updated state.
     */
    O execute(I input, O state);
}
