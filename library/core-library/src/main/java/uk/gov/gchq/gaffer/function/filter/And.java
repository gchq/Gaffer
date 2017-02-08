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
package uk.gov.gchq.gaffer.function.filter;

import uk.gov.gchq.gaffer.function.FilterFunction;
import uk.gov.gchq.gaffer.function.MultiFilterFunction;
import uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext;
import java.util.List;

/**
 * An <code>And</code> is a {@link MultiFilterFunction} that should be created with a list of
 * {@link ConsumerFunctionContext} contain {@link FilterFunction}s.
 * This filter ANDs together the filter results from all these filters and returns the result.
 *
 * @see uk.gov.gchq.gaffer.function.aggregate.NumericAggregateFunction
 */
public class And extends MultiFilterFunction {
    public And() {
    }

    public And(final List<ConsumerFunctionContext<Integer, FilterFunction>> function) {
        super(function);
    }

    @Override
    public And statelessClone() {
        return new And(cloneFunctions());
    }

    /**
     * @param input the input to test
     * @return true if all of the contained filter functions returns true, otherwise false.
     */
    @Override
    public boolean isValid(final Object[] input) {
        for (final Boolean result : executeFilters(input)) {
            if (!result) {
                return false;
            }
        }

        return true;
    }
}
