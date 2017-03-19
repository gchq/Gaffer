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

package uk.gov.gchq.gaffer.function.context;

import uk.gov.gchq.gaffer.function.ProducerFunction;
import uk.gov.gchq.gaffer.function.Tuple;
import java.util.Arrays;
import java.util.List;

/**
 * A <code>ProducerFunctionContext</code> wraps a {@link uk.gov.gchq.gaffer.function.ProducerFunction}, and provides the
 * references to project the results produced by a function into an output {@link uk.gov.gchq.gaffer.function.Tuple}.
 *
 * @param <R> The type of reference used to project data into output tuples in this context.
 * @param <F> The type of {@link uk.gov.gchq.gaffer.function.ProducerFunction} wrapped by the context.
 */
public class ProducerFunctionContext<R, F extends ProducerFunction> extends FunctionContext<F> {
    private static final long serialVersionUID = 3569894349639432082L;
    private List<R> projection;

    /**
     * Default constructor - used for serialisation.
     */
    public ProducerFunctionContext() {
    }

    /**
     * Create a <code>ProducerFunctionContext</code> that projects the results of the
     * {@link uk.gov.gchq.gaffer.function.ProducerFunction} into output {@link uk.gov.gchq.gaffer.function.Tuple}s using the specified
     * projection references.
     *
     * @param function   {@link uk.gov.gchq.gaffer.function.ProducerFunction} to be wrapped by the context.
     * @param projection References to project output data.
     */
    public ProducerFunctionContext(final F function, final List<R> projection) {
        super(function);
        setProjection(projection);
    }

    /**
     * @return References to project data into an output {@link uk.gov.gchq.gaffer.function.Tuple}.
     */
    public List<R> getProjection() {
        return projection;
    }

    /**
     * @param projection References to project data into an output {@link uk.gov.gchq.gaffer.function.Tuple}.
     */
    public void setProjection(final List<R> projection) {
        this.projection = projection;
    }

    /**
     * Project data into an output {@link uk.gov.gchq.gaffer.function.Tuple}.
     *
     * @param tuple  Output tuple to project into.
     * @param values Results to project.
     */
    public void project(final Tuple<R> tuple, final Object[] values) {
        int i = 0;
        for (final R reference : projection) {
            Object value = i < values.length ? values[i] : null;
            tuple.put(reference, value);
            i++;
        }
    }

    /**
     * Implementation of the Builder pattern for {@link uk.gov.gchq.gaffer.function.context.ProducerFunctionContext}.
     *
     * @param <R> The type of reference used to project data into output tuples in the context.
     * @param <F> The type of {@link uk.gov.gchq.gaffer.function.ProducerFunction} wrapped by the context.
     */
    public static class Builder<R, F extends ProducerFunction> extends FunctionContext.Builder<F> {
        private boolean projected = false;

        /**
         * Create a <code>Builder</code> to configure a new {@link uk.gov.gchq.gaffer.function.context.ProducerFunctionContext}.
         */
        public Builder() {
            this(new ProducerFunctionContext<R, F>());
        }

        /**
         * Create a <code>Builder</code> to configure a {@link uk.gov.gchq.gaffer.function.context.ProducerFunctionContext}.
         *
         * @param context {@link uk.gov.gchq.gaffer.function.context.ProducerFunctionContext} to be configured.
         */
        public Builder(final ProducerFunctionContext<R, F> context) {
            super(context);
        }

        @Override
        public Builder<R, F> execute(final F function) {
            return (Builder<R, F>) super.execute(function);
        }

        /**
         * Sets the references to be used by the {@link uk.gov.gchq.gaffer.function.context.ProducerFunctionContext} to project
         * output data and returns this <code>Builder</code> for further configuration.
         *
         * @param newProjection References to project data into an output {@link uk.gov.gchq.gaffer.function.Tuple}.
         * @return This <code>Builder</code>.
         */
        public Builder<R, F> project(final R... newProjection) {
            if (!projected) {
                getContext().setProjection(Arrays.asList(newProjection));
                projected = true;
            } else {
                throw new IllegalStateException("Projection has already been set");
            }

            return this;
        }

        /**
         * Tests whether the projection references have been configured.
         *
         * @return False until <code>project(R...)</code> is called, then true.
         */
        public boolean isProjected() {
            return projected;
        }

        @Override
        public ProducerFunctionContext<R, F> build() {
            return getContext();
        }

        @Override
        public ProducerFunctionContext<R, F> getContext() {
            return (ProducerFunctionContext<R, F>) super.getContext();
        }
    }
}
