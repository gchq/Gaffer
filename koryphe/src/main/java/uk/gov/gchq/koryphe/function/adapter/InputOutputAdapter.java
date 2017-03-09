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
 * A {@link Function} that applies a {@link Function} to both the input and output so that the function can be
 * applied in a different context.
 *
 * @param <I>  Type of input to be transformed
 * @param <FI> Type of input expected by the function
 * @param <F>  Type of Function to execute
 * @param <FO> Type of output produced by the function
 * @param <O>  Type of transformed output
 */
public abstract class InputOutputAdapter<I, FI, FO, O, F> extends InputAdapter<I, FI, F> {
    protected Function<FO, O> outputAdapter;

    public InputOutputAdapter(final Function<I, FI> inputAdapter, final F function, final Function<FO, O> outputAdapter) {
        super(inputAdapter, function);
        setOutputAdapter(outputAdapter);
    }

    public void setOutputAdapter(final Function<FO, O> outputAdapter) {
        this.outputAdapter = outputAdapter;
    }

    public Function<FO, O> getOutputAdapter() {
        return outputAdapter;
    }

    /**
     * Adapt the output value from the type produced by the function. If no output adapter has been specified, this
     * method assumes no transformation is required and simply casts the output to the transformed type.
     *
     * @param output Output to be transformed
     * @return Transformed output
     */
    protected O adaptOutput(FO output) {
        return outputAdapter == null ? (O) output : outputAdapter.apply(output);
    }
}
