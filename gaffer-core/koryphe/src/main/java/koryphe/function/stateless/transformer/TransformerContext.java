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

import koryphe.function.FunctionContext;

public class TransformerContext<I, FI, FO, O> extends FunctionContext<I, FI, FO, O, Transformer<FI, FO>> implements Transformer<I, O> {
    /**
     * Default constructor - for serialisation.
     */
    public TransformerContext() { }

    @Override
    public O execute(final I input) {
        return adaptToOutput(function.execute(adaptFromInput(input)));
    }

    @Override
    public TransformerContext<I, FI, FO, O> copy() {
        return copyInto(new TransformerContext<I, FI, FO, O>());
    }
}
