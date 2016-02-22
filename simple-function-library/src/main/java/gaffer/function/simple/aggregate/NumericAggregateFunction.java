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
package gaffer.function.simple.aggregate;

import gaffer.function.AggregateFunction;
import gaffer.function.SimpleAggregateFunction;

/**
 * An <code>NumericAggregateFunction</code> is a {@link SimpleAggregateFunction} that takes in
 * {@link java.lang.Number}s of the same type and processes the number in some way. To implement this class just
 * implement the init methods and aggregate methods for the different number types.
 * If you know the type of number that will be used then this can be set by calling setMode(NumberType),
 * otherwise it will be automatically set for you using the class of the first number passed in.
 *
 * @see gaffer.function.simple.aggregate.NumericAggregateFunction
 */
public abstract class NumericAggregateFunction extends SimpleAggregateFunction<Number> {

    private NumberType mode = NumberType.AUTO;

    protected Number aggregate = null;
    protected boolean allowsNull = false;

    /**
     * Sets the number type mode. If this is not set, then this will be set automatically based on the class of the
     * first number that is passed to the aggregator.
     *
     * @param mode the {@link gaffer.function.simple.aggregate.NumericAggregateFunction.NumberType} to set.
     */
    public void setMode(final NumberType mode) {
        this.mode = mode;
    }

    /**
     * @return the {@link gaffer.function.simple.aggregate.NumericAggregateFunction.NumberType} to be aggregated
     */
    public NumberType getMode() {
        return mode;
    }

    @Override
    public void init() {
        switch (mode) {
            case INT:
                initInt();
                break;
            case LONG:
                initLong();
                break;
            case DOUBLE:
                initDouble();
                break;
            default:
                aggregate = null;
        }
    }

    protected abstract void initInt();

    protected abstract void initLong();

    protected abstract void initDouble();

    @Override
    protected void _aggregate(final Number input) {
        if (!allowsNull && input == null) {
            return;
        }
        switch (mode) {
            case AUTO:
                if (input instanceof Integer) {
                    setMode(NumberType.INT);
                } else if (input instanceof Long) {
                    setMode(NumberType.LONG);
                } else if (input instanceof Double) {
                    setMode(NumberType.DOUBLE);
                } else {
                    break;
                }
                if (aggregate == null) {
                    init();
                }
                _aggregate(input);
                break;
            case INT:
                aggregateInt((Integer) input);
                break;
            case LONG:
                aggregateLong((Long) input);
                break;
            case DOUBLE:
                aggregateDouble((Double) input);
                break;
            default:
                break;
        }
    }

    protected abstract void aggregateInt(final Integer input);

    protected abstract void aggregateLong(final Long input);

    protected abstract void aggregateDouble(final Double input);

    @Override
    public Number _state() {
        return aggregate;
    }

    public enum NumberType {
        AUTO, INT, LONG, DOUBLE
    }

    public abstract AggregateFunction statelessClone();
}
