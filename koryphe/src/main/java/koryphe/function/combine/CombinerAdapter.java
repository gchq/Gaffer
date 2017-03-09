package koryphe.function.combine;

import koryphe.function.FunctionInputOutputAdapter;
import koryphe.function.transform.Transformer;

/**
 * A {@link Combiner} that applies a {@link Transformer} to the input and output so that a combiner can be applied in a
 * different context.
 *
 * @param <I> Type of input to be combined
 * @param <CI> Type of input expected by the combiner
 * @param <CO> Type of output produced by the combiner
 * @param <O> The type of transformed output
 */
public class CombinerAdapter<I, CI, CO, O> extends FunctionInputOutputAdapter<I, CI, CO, O, Combiner<CI, CO>> implements Combiner<I, O> {
    private Transformer<O, CO> outputReverse;

    /**
     * Default constructor - for serialisation.
     */
    public CombinerAdapter() {
        super(null, null, null);
    }

    public CombinerAdapter(Transformer<I, CI> inputAdapter, Combiner<CI, CO> function, Transformer<CO, O> outputAdapter, Transformer<O, CO> outputReverse) {
        super(inputAdapter, function, outputAdapter);
        setOutputReverse(outputReverse);
    }

    public Transformer<O, CO> getOutputReverse() {
        return outputReverse;
    }

    public void setOutputReverse(Transformer<O, CO> outputReverse) {
        this.outputReverse = outputReverse;
    }

    private CO reverseOutput(O output) {
        return outputReverse == null ? (CO) output : outputReverse.execute(output);
    }

    @Override
    public O execute(I input, O state) {
        return adaptOutput(function.execute(adaptInput(input), reverseOutput(state)));
    }
}
