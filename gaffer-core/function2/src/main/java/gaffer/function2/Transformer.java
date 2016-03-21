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
 * A <code>Transformer</code> {@link gaffer.function2.StatelessFunction} transforms an input value
 * to produce an output value.
 * @param <I> Function input type
 * @param <O> Function output type
 */
public abstract class Transformer<I, O> extends StatelessFunction<I, O> {
    public O execute(final I input) {
        return transform(input);
    }

    /**
     * Transform an input value to produce an output value.
     * @param input Input value
     * @return Output value
     */
    public abstract O transform(I input);

    /**
     * @return New <code>Transformer</code> of the same type.
     */
    public abstract Transformer<I, O> copy();
}
