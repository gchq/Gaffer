/*
 * Copyright 2016-2017 Crown Copyright
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
package uk.gov.gchq.koryphe.bifunction;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A {@link BiFunction} that applies a {@link Function} to the input and output so that a combiner can be applied in a
 * different context.
 *
 * @param <I>  Type of input to be combined
 * @param <FI> Type of input expected by the combiner
 * @param <FO> Type of second input to function and output produced by the function
 * @param <O>  The type of transformed output
 */
public class AdaptedBiFunction<I, FI, FO, O> implements BiFunction<I, O, O> {
    protected BiFunction<FI, FO, FO> function;
    protected Function<I, FI> inputAdapter;
    protected BiFunction<FO, O, O> outputAdapter;
    private Function<O, FO> reverseOutputAdapter;

    /**
     * Default constructor - for serialisation.
     */
    public AdaptedBiFunction() {
    }

    public AdaptedBiFunction(final BiFunction<FI, FO, FO> function,
                             final Function<I, FI> inputAdapter,
                             final BiFunction<FO, O, O> outputAdapter,
                             final Function<O, FO> reverseOutputAdapter) {
        setFunction(function);
        setInputAdapter(inputAdapter);
        setOutputAdapter(outputAdapter);
        setReverseOutputAdapter(reverseOutputAdapter);
    }

    public AdaptedBiFunction(final BiFunction<FI, FO, FO> function,
                             final Function<I, FI> inputAdapter,
                             final Function<FO, O> outputAdapter,
                             final Function<O, FO> reverseOutputAdapter) {
        this(function, inputAdapter, (fo, o) -> outputAdapter.apply(fo), reverseOutputAdapter);
    }

    @Override
    public O apply(final I input, final O state) {
        return adaptOutput(function.apply(adaptInput(input), reverseOutput(state)), state);
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
     * @param output the output to be transformed
     * @param state  the state of the transformation
     * @return Transformed output
     */
    protected O adaptOutput(final FO output, final O state) {
        return outputAdapter == null ? (O) output : outputAdapter.apply(output, state);
    }

    public void setInputAdapter(final Function<I, FI> inputAdapter) {
        this.inputAdapter = inputAdapter;
    }

    public Function<I, FI> getInputAdapter() {
        return inputAdapter;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public BiFunction<FI, FO, FO> getFunction() {
        return function;
    }

    public void setFunction(final BiFunction<FI, FO, FO> function) {
        this.function = function;
    }

    public void setOutputAdapter(final BiFunction<FO, O, O> outputAdapter) {
        this.outputAdapter = outputAdapter;
    }

    public BiFunction<FO, O, O> getOutputAdapter() {
        return outputAdapter;
    }

    public Function<O, FO> getReverseOutputAdapter() {
        return reverseOutputAdapter;
    }

    public void setReverseOutputAdapter(final Function<O, FO> outputReverse) {
        this.reverseOutputAdapter = outputReverse;
    }

    private FO reverseOutput(final O input) {
        return reverseOutputAdapter == null ? (FO) input : reverseOutputAdapter.apply(input);
    }
}
