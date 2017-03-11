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

package uk.gov.gchq.koryphe.binaryoperator;

import uk.gov.gchq.koryphe.bifunction.AdaptedBiFunction;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * An {@link java.util.function.BinaryOperator} that applies a {@link Function} to the input and output so that an aggregator can be applied
 * in a different context.
 *
 * @param <T>  Type of value to be aggregated
 * @param <FT> Type of value expected by aggregator
 */
public class AdaptedBinaryOperator<T, FT> extends AdaptedBiFunction<T, FT, FT, T> implements BinaryOperator<T> {
    public AdaptedBinaryOperator() {
    }

    public AdaptedBinaryOperator(BinaryOperator<FT> function,
                                 Function<T, FT> inputAdapter,
                                 BiFunction<FT, T, T> outputAdapter,
                                 Function<T, FT> reverseOutputAdapter) {
        super(function, inputAdapter, outputAdapter, reverseOutputAdapter);
    }
}
