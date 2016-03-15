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

package gaffer.function2;

/**
 * A <code>StatefulFunction</code> is a {@link gaffer.function2.Function} that updates state in response
 * to each input, and outputs the current state on request. The order in which Gaffer will provide input
 * values is undefined, so to provide consistent behaviour, all <code>StatefulFunction</code>s should be
 * commutative and associative.
 * @param <I> Function input type
 * @param <O> Function output type
 */
public interface StatefulFunction<I, O> extends Function<I, O> {
    /**
     * Resets the state of this <code>StatefulFunction</code>.
     */
    void init();

    /**
     * Updates the state of this <code>StatefulFunction</code>.
     * @param input Input value
     */
    void execute(I input);

    /**
     * @return Current state of this <code>StatefulFunction</code>
     */
    O state();

    /**
     * @return New <code>StatefulFunction</code> of the same type.
     */
    StatefulFunction<I, O> copy();
}
