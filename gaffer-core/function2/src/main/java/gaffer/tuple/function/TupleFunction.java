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

package gaffer.tuple.function;

import gaffer.function2.Function;
import gaffer.tuple.function.context.FunctionContext;
import gaffer.tuple.function.context.FunctionContexts;

public abstract class TupleFunction<F extends Function, R> {
    protected FunctionContexts<F, R> functions;

    /**
     * @param functions {@link gaffer.function2.StatefulFunction}s to aggregate tuple values.
     */
    public void setFunctions(final FunctionContexts<F, R> functions) {
        this.functions = functions;
    }

    /**
     * @return {@link gaffer.function2.StatefulFunction}s to aggregate tuple values.
     */
    public FunctionContexts<F, R> getFunctions() {
        return functions;
    }

    /**
     * @param function {@link gaffer.function2.StatefulFunction} to aggregate tuple values.
     */
    public void addFunction(final FunctionContext<F, R> function) {
        if (functions == null) {
            functions = new FunctionContexts<F, R>();
        }
        functions.add(function);
    }
}
