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

package gaffer.tuple.function.context;

import gaffer.function2.Function;
import gaffer.tuple.Tuple;
import gaffer.tuple.view.Reference;

import java.util.ArrayList;

/**
 * A {@link java.util.List} of {@link gaffer.tuple.function.context.FunctionContext}s.
 */
public class FunctionContexts<F extends Function, R> extends ArrayList<FunctionContext<F, R>> {
    private static final long serialVersionUID = 43876759563583725L;

    public boolean assignableFrom(final Object schemaTuple) {
        if (!(schemaTuple instanceof Tuple)) {
            throw new IllegalArgumentException("Tuple functions must be supplied with tuple of types.");
        }
        for (FunctionContext context : this) {
            if (!context.assignableFrom((Tuple<R>) schemaTuple)) {
                return false;
            }
        }
        return true;
    }

    public boolean assignableTo(final Object schemaTuple) {
        if (!(schemaTuple instanceof Tuple)) {
            throw new IllegalArgumentException("Tuple functions must be supplied with tuple of types.");
        }
        for (FunctionContext context : this) {
            if (!context.assignableTo((Tuple<R>) schemaTuple)) {
                return false;
            }
        }
        return true;
    }

    public FunctionContexts<F, R> copy() {
        FunctionContexts<F, R> newContexts = new FunctionContexts<>();
        for (FunctionContext<F, R> context : this) {
            newContexts.add(context.copy());
        }
        return newContexts;
    }

    public static class Builder<F extends Function, R> {
        private FunctionContexts<F, R> contexts;
        private FunctionContext.Builder<F, R> contextBuilder = null;

        public Builder() {
            contexts = new FunctionContexts<>();
        }

        public Builder<F, R> select(final R... references) {
            createContext();
            contextBuilder.select(references);
            return this;
        }

        public Builder<F, R> select(final Reference<R> reference) {
            createContext();
            contextBuilder.select(reference);
            return this;
        }

        public Builder<F, R> execute(final F function) {
            createContext();
            contextBuilder.execute(function);
            return this;
        }

        public Builder<F, R> project(final R... references) {
            createContext();
            contextBuilder.project(references);
            return this;
        }

        public Builder<F, R> project(final Reference<R> reference) {
            createContext();
            contextBuilder.project(reference);
            return this;
        }

        public Builder<F, R> add() {
            if (contextBuilder != null) {
                contexts.add(contextBuilder.build());
                contextBuilder = null;
            }
            return this;
        }

        public FunctionContexts<F, R> build() {
            return contexts.copy();
        }

        private void createContext() {
            if (contextBuilder == null) {
                contextBuilder = new FunctionContext.Builder<F, R>();
            }
        }
    }
}
