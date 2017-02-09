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

import uk.gov.gchq.gaffer.function.ConsumerFunction;
import uk.gov.gchq.gaffer.function.Tuple;
import java.util.Arrays;
import java.util.List;

/**
 * A <code>ConsumerFunctionContext</code> wraps a {@link uk.gov.gchq.gaffer.function.ConsumerFunction}, and provides the
 * references to select data from an input {@link uk.gov.gchq.gaffer.function.Tuple} so they can be consumed by the function.
 *
 * @param <R> The type of reference used to select data from input tuples in this context.
 * @param <F> The type of {@link uk.gov.gchq.gaffer.function.ConsumerFunction} wrapped by the context.
 */
public class ConsumerFunctionContext<R, F extends ConsumerFunction> extends FunctionContext<F> {
    private static final long serialVersionUID = -4572706365436487862L;
    private List<R> selection;
    private Object[] selected;

    /**
     * Default constructor - used for serialisation.
     */
    public ConsumerFunctionContext() {
        super();
    }

    /**
     * Create a <code>ConsumerFunctionContext</code> that selects data from input {@link uk.gov.gchq.gaffer.function.Tuple}s using
     * the specified selection references to pass arguments to the {@link uk.gov.gchq.gaffer.function.ConsumerFunction}.
     *
     * @param function  {@link uk.gov.gchq.gaffer.function.ConsumerFunction} to be wrapped by the context.
     * @param selection References to select input data.
     */
    public ConsumerFunctionContext(final F function, final List<R> selection) {
        super(function);
        setSelection(selection);
    }

    /**
     * @return References to select data from an input {@link uk.gov.gchq.gaffer.function.Tuple}.
     */
    public List<R> getSelection() {
        return selection;
    }

    /**
     * @param selection References to select data from an input {@link uk.gov.gchq.gaffer.function.Tuple}.
     */
    public void setSelection(final List<R> selection) {
        this.selection = selection;
        selected = new Object[null != selection ? selection.size() : 0];
    }

    /**
     * Select data from an input {@link uk.gov.gchq.gaffer.function.Tuple}.
     * <p>
     * <b>Note</b>: due to re-use of the container used to return input data, this method is not thread safe.
     *
     * @param tuple Input tuple to select from.
     * @return Selected values.
     */
    public Object[] select(final Tuple<R> tuple) {
        int i = 0;
        for (final R reference : selection) {
            selected[i++] = tuple.get(reference);
        }

        return selected;
    }

    /**
     * Implementation of the Builder pattern for {@link uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext}.
     *
     * @param <R> The type of reference used to select data from input tuples in the context.
     * @param <F> The type of {@link uk.gov.gchq.gaffer.function.ConsumerFunction} wrapped by the context.
     */
    public static class Builder<R, F extends ConsumerFunction> extends FunctionContext.Builder<F> {
        private boolean selected = false;

        /**
         * Create a <code>Builder</code> to configure a new {@link uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext}.
         */
        public Builder() {
            this(new ConsumerFunctionContext<R, F>());
        }

        /**
         * Create a <code>Builder</code> to configure a {@link uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext}.
         *
         * @param context {@link uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext} to be configured.
         */
        public Builder(final ConsumerFunctionContext<R, F> context) {
            super(context);
        }

        /**
         * Sets the references to be used by the {@link uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext} to select
         * input data and returns this <code>Builder</code> for further configuration.
         *
         * @param newSelection References to select data from an input {@link uk.gov.gchq.gaffer.function.Tuple}.
         * @return This <code>Builder</code>.
         */
        public Builder<R, F> select(final R... newSelection) {
            if (!selected) {
                getContext().setSelection(Arrays.asList(newSelection));
                selected = true;
            } else {
                throw new IllegalStateException("Selection has already been set");
            }
            return this;
        }

        @Override
        public Builder<R, F> execute(final F function) {
            return (Builder<R, F>) super.execute(function);
        }

        /**
         * Tests whether the selection references have been configured.
         *
         * @return False until <code>select(R...)</code> is called, then true.
         */
        public boolean isSelected() {
            return selected;
        }

        @Override
        public ConsumerFunctionContext<R, F> build() {
            return getContext();
        }

        @Override
        public ConsumerFunctionContext<R, F> getContext() {
            return (ConsumerFunctionContext<R, F>) super.getContext();
        }
    }
}
