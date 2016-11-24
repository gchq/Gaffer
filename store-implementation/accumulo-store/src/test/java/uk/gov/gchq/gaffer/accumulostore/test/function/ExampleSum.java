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

package uk.gov.gchq.gaffer.accumulostore.test.function;

import uk.gov.gchq.gaffer.function.SimpleAggregateFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import uk.gov.gchq.gaffer.function.annotation.Outputs;

@Inputs(Integer.class)
@Outputs(Integer.class)
public class ExampleSum extends SimpleAggregateFunction<Integer> {
    private Integer aggregate = null;

    @Override
    public void init() {
        aggregate = null;
    }

    @Override
    protected void _aggregate(final Integer input) {
        if (input != null) {
            if (aggregate == null) {
                aggregate = input;
            } else {
                aggregate = aggregate + input;
            }
        }
    }

    @Override
    public Integer _state() {
        return aggregate;
    }

    public ExampleSum statelessClone() {
        return new ExampleSum();
    }
}