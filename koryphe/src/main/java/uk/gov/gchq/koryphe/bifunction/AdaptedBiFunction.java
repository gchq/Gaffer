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

    public AdaptedBiFunction(BiFunction<FI, FO, FO> function,
                             Function<I, FI> inputAdapter,
                             BiFunction<FO, O, O> outputAdapter,
                             Function<O, FO> reverseOutputAdapter) {
        setFunction(function);
        setInputAdapter(inputAdapter);
        setOutputAdapter(outputAdapter);
        setReverseOutputAdapter(reverseOutputAdapter);
    }

    @Override
    public O apply(I input, O state) {
        return adaptOutput(function.apply(adaptInput(input), reverseOutput(state)), state);
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

    /**
     * Adapt the output value from the type produced by the function. If no output adapter has been specified, this
     * method assumes no transformation is required and simply casts the output to the transformed type.
     *
     * @param output Output to be transformed
     * @return Transformed output
     */
    protected O adaptOutput(FO output, O state) {
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

    public void setFunction(BiFunction<FI, FO, FO> function) {
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

    public void setReverseOutputAdapter(Function<O, FO> outputReverse) {
        this.reverseOutputAdapter = outputReverse;
    }

    private FO reverseOutput(O input) {
        return reverseOutputAdapter == null ? (FO) input : reverseOutputAdapter.apply(input);
    }
}
