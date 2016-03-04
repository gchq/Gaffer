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
 * A <code>Buffer</code> {@link gaffer.function2.StatefulFunction} has different input and
 * output types.
 * @param <I> Function input type
 * @param <O> Function output type
 */
public abstract class Buffer<I, O> implements StatefulFunction<I, O> {
    public void execute(final I input) {
        accept(input);
    }

    /**
     * Accept a new input value into this <code>Buffer</code>.
     * @param input Input value
     */
    public abstract void accept(I input);

    /**
     * @return New <code>Buffer</code> of the same type.
     */
    public abstract Buffer<I, O> copy();
}
