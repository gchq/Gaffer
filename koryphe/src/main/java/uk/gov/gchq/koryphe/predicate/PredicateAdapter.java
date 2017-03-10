package uk.gov.gchq.koryphe.predicate;

import uk.gov.gchq.koryphe.adapter.InputAdapter;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A {@link Predicate} that applies a {@link Function} to the input so that the predicate can be applied in a
 * different context.
 *
 * @param <I>  Type of input to be transformed
 * @param <VI> Type of input expected by the predicate
 */
public class PredicateAdapter<I, VI> extends InputAdapter<I, VI, Predicate<VI>> implements IKoryphePredicate<I> {
    /**
     * Default constructor - for serialisation.
     */
    public PredicateAdapter() {
        super(null, null);
    }

    public PredicateAdapter(Function<I, VI> inputAdapter, Predicate<VI> predicate) {
        super(inputAdapter, predicate);
    }

    @Override
    public boolean test(I input) {
        return function.test(adaptInput(input));
    }
}
