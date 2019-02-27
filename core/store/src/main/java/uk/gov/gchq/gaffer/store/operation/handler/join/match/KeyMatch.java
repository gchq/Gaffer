package uk.gov.gchq.gaffer.store.operation.handler.join.match;

import uk.gov.gchq.gaffer.operation.impl.join.match.Match;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;


/**
 * A {@code KeyMatch} is a {@code Match} which takes two key functions which are used to extract keys from
 * a left and right side input. A match occurs when the keys from the left and right are equal.
 */
public class KeyMatch implements Match {

    private static final String NULL_FUNCTION_ERROR_MESSAGE = "You must specify a key function for the left and right input";
    private Function leftKeyFunction;
    private Function rightKeyFunction;

    public KeyMatch(final Function leftKeyFunction, final Function rightKeyFunction) {
        this.leftKeyFunction = leftKeyFunction;
        this.rightKeyFunction = rightKeyFunction;
    }

    public Function getLeftKeyFunction() {
        return leftKeyFunction;
    }

    public void setLeftKeyFunction(final Function leftKeyFunction) {
        this.leftKeyFunction = leftKeyFunction;
    }

    public Function getRightKeyFunction() {
        return rightKeyFunction;
    }

    public void setRightKeyFunction(final Function rightKeyFunction) {
        this.rightKeyFunction = rightKeyFunction;
    }

    @Override
    public List matching(Object testObject, List testList) {
        ArrayList<Object> matching = new ArrayList<>();

        if (this.leftKeyFunction == null || this.rightKeyFunction == null) {
            throw new IllegalArgumentException(NULL_FUNCTION_ERROR_MESSAGE);
        }

        Object leftHandCriteria = leftKeyFunction.apply(testObject);
        Object rightHandCriteria;
        for (final Object rightHandObject : testList) {
            rightHandCriteria = rightKeyFunction.apply(rightHandObject);
            if (rightHandCriteria.equals(leftHandCriteria)) {
                matching.add(rightHandObject);
            }
        }

        return matching;
    }

    public static final class Builder {
        private Function leftKeyFunction;
        private Function rightKeyFunction;

        public KeyMatch build() {
            return new KeyMatch(leftKeyFunction, rightKeyFunction);
        }

        public Builder leftHandFunction(final Function leftKeyFunction) {
            this.leftKeyFunction = leftKeyFunction;
            return this;
        }

        public Builder rightHandFunction(final Function rightKeyFunction) {
            this.rightKeyFunction = rightKeyFunction;
            return this;
        }
    }
}
