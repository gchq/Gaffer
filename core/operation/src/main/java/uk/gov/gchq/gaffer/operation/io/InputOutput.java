/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.io;

/**
 * {@code InputOutput} operations are Gaffer operations which consume a single
 * input and transforms that input into an output type.
 *
 * This is a marker interface composed of the {@link Input} and {@link Output} interfaces.
 *
 * @param <I> the type of the operation input
 * @param <O> the type of the operation output
 */
public interface InputOutput<I, O> extends Input<I>, Output<O> {

    interface Builder<OP extends InputOutput<I, O>, I, O, B extends Builder<OP, I, O, ?>>
            extends
            Input.Builder<OP, I, B>,
            Output.Builder<OP, O, B> {
    }
}
