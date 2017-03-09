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

package koryphe.function.aggregate;

import koryphe.function.FunctionInputOutputAdapter;
import koryphe.function.combine.Combiner;
import koryphe.function.combine.CombinerAdapter;
import koryphe.function.transform.Transformer;

/**
 * An {@link Aggregator} that applies a {@link Transformer} to the input and output so that an aggregator can be applied
 * in a different context.
 *
 * @param <T> Type of value to be aggregated
 * @param <FT> Type of value expected by aggregator
 */
public class AggregatorAdapter<T, FT> extends FunctionInputOutputAdapter<T, FT, FT, T, Aggregator<FT>> implements Aggregator<T> {
    /**
     * Default constructor - for serialisation.
     */
    public AggregatorAdapter() {
        super(null, null, null);
    }

    public AggregatorAdapter(Transformer<T, FT> inputAdapter, Aggregator<FT> function, Transformer<FT, T> outputAdapter) {
        super(inputAdapter, function, outputAdapter);
    }

    @Override
    public T execute(T input, T state) {
        return adaptOutput(function.execute(adaptInput(input), adaptInput(state)));
    }
}
