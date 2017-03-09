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

package uk.gov.gchq.koryphe.function.adapter;

import java.util.function.Function;

/**
 * A {@link Function} that applies a {@link Function} to the input so that the function can be applied in a different
 * context.
 *
 * @param <I>  Type of input to be transformed
 * @param <FI> Type of input expected by the function
 * @param <F>  Type of Function to execute
 */
public abstract class InputAdapter<I, FI, F> {
    protected Function<I, FI> inputAdapter;
    protected F function;

    public InputAdapter(final Function<I, FI> inputAdapter, final F function) {
        setInputAdapter(inputAdapter);
    }

    public F getFunction() {
        return function;
    }

    public void setFunction(F function) {
        this.function = function;
    }

    public void setInputAdapter(final Function<I, FI> inputAdapter) {
        this.inputAdapter = inputAdapter;
    }

    public Function<I, FI> getInputAdapter() {
        return inputAdapter;
    }

    /**
     * Adapt the input value to the type expected by the function. If no input adapter has been specified, this method
     * assumes no transformation is required and simply casts the input to the transformed type.
     *
     * @param input Input to be transformed
     * @return Transformed input
     */
    protected FI adaptInput(I input) {
        return inputAdapter == null ? (FI) input : inputAdapter.apply(input);
    }
}
