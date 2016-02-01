package gaffer.function.simple.filter;

import gaffer.function.FilterFunction;
import gaffer.function.annotation.Inputs;

/**
 * An <code>IsXLessThanY</code> is a {@link gaffer.function.FilterFunction} that checks that the first input
 * {@link java.lang.Comparable} is less than the second input {@link java.lang.Comparable}.
 */
@Inputs({Comparable.class, Comparable.class})
public class IsXLessThanY extends FilterFunction {
    public IsXLessThanY statelessClone() {
        return new IsXLessThanY();
    }

    protected boolean filter(Object[] input) {
        return !(null == input
                || input.length != 2
                || null == input[0]
                || null == input[1]
                || !(input[0] instanceof Comparable)
                || input[0].getClass() != input[1].getClass())
                && ((Comparable) input[0]).compareTo(input[1]) < 0;
    }
}
