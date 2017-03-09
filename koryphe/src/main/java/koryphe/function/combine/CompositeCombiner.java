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

package koryphe.function.combine;

import koryphe.function.CompositeFunction;

/**
 * A composite {@link Combiner} that applies each combiner in turn, supplying the result of each combiner as
 * the state of the next, and returning the result of the last combiner. Combiner input/output types are assumed
 * to be compatible - no checking is done, and a class cast exception will be thrown if incompatible combiners are
 * executed.
 * @param <I> Type of input of first combiner
 * @param <O> Type of output (all combiners must share compatible output types)
 */
public final class CompositeCombiner<I, O> extends CompositeFunction<Combiner> implements Combiner<I, O> {
    @Override
    public O execute(final I input, final O state) {
        Object result = state;
        for (Combiner function : this) {
            result = function.execute(input, result);
        }
        return (O) result;
    }
}
