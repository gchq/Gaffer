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
package uk.gov.gchq.gaffer.function.simple.filter;

import uk.gov.gchq.gaffer.function.FilterFunction;

/**
 * An <code>Not</code> is a {@link FilterFunction} that wraps a {@link FilterFunction},
 * and inverts the result from the wrapped function.
 *
 * @see uk.gov.gchq.gaffer.function.simple.aggregate.NumericAggregateFunction
 */
public class Not extends FilterFunction {
    private FilterFunction function;

    public Not() {
    }

    public Not(final FilterFunction function) {
        this.function = function;
    }

    @Override
    public Not statelessClone() {
        return new Not(function.statelessClone());
    }

    @Override
    public Class<?>[] getInputClasses() {
        return function.getInputClasses();
    }

    /**
     * @param input the input to test
     * @return the inverted result from the wrapped filter function.
     */
    @Override
    public boolean isValid(final Object[] input) {
        return null == function || !function.isValid(input);

    }

    public FilterFunction getFunction() {
        return function;
    }

    public void setFunction(final FilterFunction function) {
        this.function = function;
    }
}
