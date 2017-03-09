package koryphe.function.validate;

import koryphe.function.FunctionInputAdapter;
import koryphe.function.transform.Transformer;

/**
 * A {@link Validator} that applies a {@link Transformer} to the input so that the validator can be applied in a
 * different context.
 *
 * @param <I> Type of input to be transformed
 * @param <VI> Type of input expected by the validator
 */
public class ValidatorAdapter<I, VI> extends FunctionInputAdapter<I, VI, Boolean, Validator<VI>> implements Validator<I> {
    /**
     * Default constructor - for serialisation.
     */
    public ValidatorAdapter() {
        super(null, null);
    }

    public ValidatorAdapter(Transformer<I, VI> inputAdapter, Validator<VI> validator) {
        super(inputAdapter, validator);
    }

    @Override
    public Boolean execute(I input) {
        return function.execute(adaptInput(input));
    }
}
