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

package gaffer.accumulostore.test.function;

import gaffer.function.SimpleAggregateFunction;
import gaffer.function.annotation.Inputs;
import gaffer.function.annotation.Outputs;

@Inputs(Long.class)
@Outputs(Long.class)
public class ExampleSumLong extends SimpleAggregateFunction<Long> {
    private Long aggregate = null;

    @Override
    public void init() {
        aggregate = 0L;
    }

    @Override
    protected void _aggregate(final Long input) {
        if (aggregate == null) {
            init();
        }

        if (input != null) {
            aggregate = aggregate + input;
        }
    }

    @Override
    public Long _state() {
        return aggregate;
    }

    public ExampleSumLong statelessClone() {
        return new ExampleSumLong();
    }
}