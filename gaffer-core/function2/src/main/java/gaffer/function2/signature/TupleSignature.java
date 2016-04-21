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

package gaffer.function2.signature;

import gaffer.function2.Function;
import gaffer.tuple.Tuple;
import gaffer.tuple.function.context.FunctionContext;
import gaffer.tuple.function.context.FunctionContexts;

/**
 * A <code>TupleSignature</code> allows the {@link Signature}s of a number of {@link Function}s to be tested in context.
 */
public class TupleSignature extends Signature {
    private FunctionContexts<? extends Function, ?> contexts;

    /**
     * Create a <code>TupleSignature</code> with the given {@link FunctionContexts}.
     * @param contexts Functions to test in context.
     */
    TupleSignature(final FunctionContexts contexts) {
        this.contexts = contexts;
    }

    @Override
    public boolean assignable(final Object arguments, final boolean reverse) {
        for (FunctionContext context : contexts) {
            Function function = context.getFunction();
            Signature functionSignature = reverse ? Signature.getOutputSignature(function) : Signature.getInputSignature(function);
            Object selection = reverse ? context.getProjectionView().select((Tuple) arguments) : context.select((Tuple) arguments);
            if (!functionSignature.assignable(selection, reverse)) {
                return false;
            }
        }
        return true;
    }
}
