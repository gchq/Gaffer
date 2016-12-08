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

import uk.gov.gchq.gaffer.function.annotation.Inputs;
import uk.gov.gchq.gaffer.function.annotation.Outputs;

/**
 * An <code>Max</code> is a {@link uk.gov.gchq.gaffer.function.SimpleAggregateFunction} that takes in
 * {@link Number}s of the same type and calculates the maximum.
 * If you know the type of number that will be used then this can be set by calling setMode(NumberType),
 * otherwise it will be automatically set for you using the class of the first number passed in.
 *
 * @see NumericAggregateFunction
 */
@Inputs(Number.class)
@Outputs(Number.class)
public class Max extends NumericAggregateFunction {
    @Override
    protected void aggregateInt(final Integer input) {
        if (input > (Integer) aggregate) {
            aggregate = input;
        }
    }

    @Override
    protected void aggregateLong(final Long input) {
        if (input > (Long) aggregate) {
            aggregate = input;
        }
    }

    @Override
    protected void aggregateDouble(final Double input) {
        if (input > (Double) aggregate) {
            aggregate = input;
        }
    }

    public Max statelessClone() {
        Max max = new Max();
        max.setMode(super.getMode());
        max.init();
        return max;
    }
}
