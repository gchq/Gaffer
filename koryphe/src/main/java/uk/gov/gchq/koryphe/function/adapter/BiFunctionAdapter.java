package uk.gov.gchq.koryphe.function.adapter;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A {@link BiFunction} that applies a {@link Function} to the input and output so that a combiner can be applied in a
 * different context.
 *
 * @param <I>  Type of input to be combined
 * @param <CI> Type of input expected by the combiner
 * @param <CO> Type of output produced by the combiner
 * @param <O>  The type of transformed output
 */
public class BiFunctionAdapter<I, CI, CO, O> extends InputOutputAdapter<I, CI, CO, O, BiFunction<CI, CO, CO>> implements BiFunction<I, O, O> {
    private Function<O, CO> outputReverse;

    /**
     * Default constructor - for serialisation.
     */
    public BiFunctionAdapter() {
        super(null, null, null);
    }

    public BiFunctionAdapter(Function<I, CI> inputAdapter, BiFunction<CI, CO, CO> function, Function<CO, O> outputAdapter, Function<O, CO> outputReverse) {
        super(inputAdapter, function, outputAdapter);
        setOutputReverse(outputReverse);
    }

    public Function<O, CO> getOutputReverse() {
        return outputReverse;
    }

    public void setOutputReverse(Function<O, CO> outputReverse) {
        this.outputReverse = outputReverse;
    }

    private CO reverseOutput(O output) {
        return outputReverse == null ? (CO) output : outputReverse.apply(output);
    }

    @Override
    public O apply(I input, O state) {
        return adaptOutput(function.apply(adaptInput(input), reverseOutput(state)));
    }
}
