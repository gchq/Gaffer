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

package uk.gov.gchq.koryphe.function;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A {@link Function} that applies a {@link Function} to both the input and output so that the function can be
 * applied in a different context.
 *
 * @param <I>  Type of input to be transformed
 * @param <FI> Type of input expected by the function
 * @param <FO> Type of output produced by the function
 * @param <O>  Type of transformed output
 */
public abstract class AdaptedFunction<I, FI, FO, O> implements Function<I, O> {
    protected Function<FI, FO> function;
    protected Function<I, FI> inputAdapter;
    protected BiFunction<FO, I, O> outputAdapter;

    public AdaptedFunction() {
    }

    public AdaptedFunction(final Function<FI, FO> function,
                           final Function<I, FI> inputAdapter,
                           final BiFunction<FO, I, O> outputAdapter) {
        setInputAdapter(inputAdapter);
        setFunction(function);
        setOutputAdapter(outputAdapter);
    }

    public AdaptedFunction(final Function<FI, FO> function,
                           final Function<I, FI> inputAdapter,
                           final Function<FO, O> outputAdapter) {
        this(function, inputAdapter, (fo, i) -> outputAdapter.apply(fo));
    }

    @Override
    public O apply(final I input) {
        return adaptOutput(function.apply(adaptInput(input)), input);
    }

    /**
     * Adapt the input value to the type expected by the function. If no input adapter has been specified, this method
     * assumes no transformation is required and simply casts the input to the transformed type.
     *
     * @param input Input to be transformed
     * @return Transformed input
     */
    protected FI adaptInput(final I input) {
        return inputAdapter == null ? (FI) input : inputAdapter.apply(input);
    }

    /**
     * Adapt the output value from the type produced by the function. If no output adapter has been specified, this
     * method assumes no transformation is required and simply casts the output to the transformed type.
     *
     * @param output Output to be transformed
     * @param state  state of function - this will be the input value
     * @return Transformed output
     */
    protected O adaptOutput(final FO output, final I state) {
        return outputAdapter == null ? (O) output : outputAdapter.apply(output, state);
    }

    public void setInputAdapter(final Function<I, FI> inputAdapter) {
        this.inputAdapter = inputAdapter;
    }

    public Function<I, FI> getInputAdapter() {
        return inputAdapter;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public Function<FI, FO> getFunction() {
        return function;
    }

    public void setFunction(final Function<FI, FO> function) {
        this.function = function;
    }

    public void setOutputAdapter(final BiFunction<FO, I, O> outputAdapter) {
        this.outputAdapter = outputAdapter;
    }

    public BiFunction<FO, I, O> getOutputAdapter() {
        return outputAdapter;
    }
}
