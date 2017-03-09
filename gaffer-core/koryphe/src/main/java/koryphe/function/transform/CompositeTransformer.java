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

package koryphe.function.transform;

import koryphe.function.CompositeFunction;

/**
 * A composite {@link Transformer} that applies each transformer in turn, supplying the result of each transformer as
 * the input to the next, and returning the result of the last transformer. Transformer input/output types are assumed
 * to be compatible - no checking is done, and a class cast exception will be thrown if incompatible transformers are
 * executed.
 * @param <I> Type of input of first transformer
 * @param <O> Type of output of last transformer
 */
public final class CompositeTransformer<I, O> extends CompositeFunction<Transformer> implements Transformer<I, O> {
    @Override
    public O execute(final I input) {
        Object result = input;
        for (Transformer function : this) {
            result = function.execute(result);
        }
        return (O) result;
    }
}
