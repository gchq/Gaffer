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

package koryphe.function.stateless.transformer;

import koryphe.function.MultiFunction;

/**
 * A {@link Transformer} that applies a list of transformers. The output of the first transformer becomes the input of
 * the next and so on until an output value is returned from the final transformer.
 * @param <I> Input type
 * @param <O> Output type
 */
public final class MultiTransformer<I, O> extends MultiFunction<Transformer> implements Transformer<I, O> {
    @Override
    public O execute(final I input) {
        Object result = input;
        for (Transformer transformer : functions) {
            result = transformer.execute(result);
        }
        return (O) result;
    }
}
