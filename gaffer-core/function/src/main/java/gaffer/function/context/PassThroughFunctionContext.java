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

package gaffer.function.context;

import gaffer.function.ConsumerProducerFunction;
import gaffer.function.Tuple;
import java.util.List;

/**
 * A <code>PassThroughFunctionContext</code> extends a {@link gaffer.function.context.ConsumerFunctionContext} to wrap
 * a {@link gaffer.function.ConsumerProducerFunction} where the consumed and produced data have the same structure
 * (they have the same references and types). This is effectively equivalent to a
 * {@link gaffer.function.context.ConsumerProducerFunctionContext} where the selection and projection references must
 * be identical - <code>PassThroughFunctionContext</code> enforces that constraint.
 *
 * @param <R> The type of reference used to select from and project into tuples in this context.
 * @param <F> The type of {@link gaffer.function.ConsumerProducerFunction} wrapped by the context.
 */
public class PassThroughFunctionContext<R, F extends ConsumerProducerFunction> extends ConsumerFunctionContext<R, F> {
    private static final long serialVersionUID = -823429611092360995L;

    /**
     * Default constructor - used for serialisation.
     */
    public PassThroughFunctionContext() {
    }

    /**
     * Create a <code>PassThroughFunctionContext</code> that wraps a
     * {@link gaffer.function.ConsumerProducerFunction}, using the specified references for selection and projection.
     *
     * @param function  {@link gaffer.function.ConsumerProducerFunction} to be wrapped by the context.
     * @param selection References to select and project data.
     */
    public PassThroughFunctionContext(final F function, final List<R> selection) {
        super(function, selection);
    }

    /**
     * Project data into an output {@link gaffer.function.Tuple}.
     *
     * @param tuple  Output tuple to project into.
     * @param values Results to project.
     */
    public void project(final Tuple<R> tuple, final Object[] values) {
        int i = 0;
        for (R reference : getSelection()) {
            Object value = i < values.length ? values[i] : null;
            tuple.put(reference, value);
            i++;
        }
    }

    /**
     * Implementation of the Builder pattern for {@link gaffer.function.context.PassThroughFunctionContext}.
     *
     * @param <R> The type of reference used to select from and project into tuples in the context.
     * @param <F> The type of {@link gaffer.function.ConsumerProducerFunction} wrapped by the context.
     */
    public static class Builder<R, F extends ConsumerProducerFunction> extends ConsumerFunctionContext.Builder<R, F> {
        /**
         * Create a <code>Builder</code> to configure a new
         * {@link gaffer.function.context.PassThroughFunctionContext}.
         */
        public Builder() {
            this(new PassThroughFunctionContext<R, F>());
        }

        /**
         * Create a <code>Builder</code> to configure a {@link gaffer.function.context.PassThroughFunctionContext}.
         *
         * @param context {@link gaffer.function.context.PassThroughFunctionContext} to be configured.
         */
        public Builder(final PassThroughFunctionContext<R, F> context) {
            super(context);
        }

        @Override
        public Builder<R, F> select(final R... newSelection) {
            return (Builder<R, F>) super.select(newSelection);
        }

        @Override
        public Builder<R, F> execute(final F function) {
            return (Builder<R, F>) super.execute(function);
        }

        @Override
        public PassThroughFunctionContext<R, F> build() {
            return getContext();
        }

        @Override
        public PassThroughFunctionContext<R, F> getContext() {
            return (PassThroughFunctionContext<R, F>) super.getContext();
        }
    }
}
