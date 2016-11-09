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

import gaffer.function.SimpleAggregateFunction;
import gaffer.function.annotation.Inputs;
import gaffer.function.annotation.Outputs;

/**
 * An <code>ComparableMax</code> is a {@link SimpleAggregateFunction} that takes in
 * {@link java.lang.Comparable}s and calculates the maximum comparable. It assumes that all the input comparables
 * are compatible and can be compared against each other.
 */
@Inputs(Comparable.class)
@Outputs(Comparable.class)
public class ComparableMax extends SimpleAggregateFunction<Comparable> {
    private Comparable aggregate;

    @Override
    public void init() {
        aggregate = null;
    }

    @Override
    protected Comparable _state() {
        return aggregate;
    }

    @Override
    protected void _aggregate(final Comparable input) {
        if (aggregate == null || input.compareTo(aggregate) > 0) {
            aggregate = input;
        }
    }

    @Override
    public ComparableMax statelessClone() {
        final ComparableMax max = new ComparableMax();
        max.init();
        return max;
    }
}
