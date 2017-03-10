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
package uk.gov.gchq.gaffer.function.aggregate;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.koryphe.binaryoperator.KorpheBinaryOperator;
import java.util.function.BinaryOperator;

/**
 * An <code>NumericAggregateFunction</code> is a {@link BinaryOperator} that takes in
 * {@link Number}s of the same type and processes the number in some way. To implement this class just
 * implement the init methods and aggregate methods for the different number types.
 * If you know the type of number that will be used then this can be set by calling setMode(NumberType),
 * otherwise it will be automatically set for you using the class of the first number passed in.
 *
 * @see NumericAggregateFunction
 */
public abstract class NumericAggregateFunction extends KorpheBinaryOperator<Number> {

    private NumberType mode = NumberType.AUTO;

    /**
     * Sets the number type mode. If this is not set, then this will be set automatically based on the class of the
     * first number that is passed to the aggregator.
     *
     * @param mode the {@link NumberType} to set.
     */
    public void setMode(final NumberType mode) {
        this.mode = mode;
    }

    /**
     * @return the {@link NumberType} to be aggregated
     */
    public NumberType getMode() {
        return mode;
    }

    @Override
    public Number apply(final Number input1, final Number input2) {
        if (input1 == null) {
            return input2;
        }

        if (input2 == null) {
            return input1;
        }

        switch (mode) {
            case AUTO:
                if (input1 instanceof Integer) {
                    setMode(NumberType.INT);
                } else if (input1 instanceof Long) {
                    setMode(NumberType.LONG);
                } else if (input1 instanceof Double) {
                    setMode(NumberType.DOUBLE);
                } else {
                    break;
                }
                return apply(input1, input2);
            case INT:
                return aggregateInt((Integer) input1, (Integer) input2);
            case LONG:
                return aggregateLong((Long) input1, (Long) input2);
            case DOUBLE:
                return aggregateDouble((Double) input1, (Double) input2);
            default:
                return null;
        }

        return null;
    }

    protected abstract Integer aggregateInt(final Integer input1, final Integer input2);

    protected abstract Long aggregateLong(final Long input1, final Long input2);

    protected abstract Double aggregateDouble(final Double input1, final Double input2);

    public enum NumberType {
        AUTO, INT, LONG, DOUBLE
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final NumericAggregateFunction that = (NumericAggregateFunction) o;

        return new EqualsBuilder()
                .append(mode, that.mode)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(mode)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("mode", mode)
                .toString();
    }
}
