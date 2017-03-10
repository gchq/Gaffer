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

/**
 * An <code>Min</code> is a {@link java.util.function.BinaryOperator} that takes in
 * {@link Number}s of the same type and calculates the sum.
 * If you know the type of number that will be used then this can be set by calling setMode(NumberType),
 * otherwise it will be automatically set for you using the class of the first number passed in.
 *
 * @see NumericAggregateFunction
 */
public class Sum extends NumericAggregateFunction {
    @Override
    protected Integer aggregateInt(final Integer input1, final Integer input2) {
        return input1 + input2;
    }

    @Override
    protected Long aggregateLong(final Long input1, final Long input2) {
        return input1 + input2;
    }

    @Override
    protected Double aggregateDouble(final Double input1, final Double input2) {
        return input1 + input2;
    }
}
