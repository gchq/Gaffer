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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import uk.gov.gchq.gaffer.function.Function;
import java.io.Serializable;

/**
 * A FunctionContext wraps a {@link uk.gov.gchq.gaffer.function.Function}. It appends application-specific configuration data to
 * the function so that it can be executed in the context of that application.
 *
 * @param <F> The type of {@link uk.gov.gchq.gaffer.function.Function} wrapped by the context.
 * @see uk.gov.gchq.gaffer.function.processor.Processor for details of how FunctionContext is used.
 */
public abstract class FunctionContext<F extends Function> implements Serializable {
    private static final long serialVersionUID = -3469570249850928140L;
    private F function;

    /**
     * Default constructor - used for serialisation.
     */
    public FunctionContext() {

    }

    /**
     * Create a FunctionContext that wraps a given {@link uk.gov.gchq.gaffer.function.Function}.
     *
     * @param function {@link uk.gov.gchq.gaffer.function.Function} to be wrapped by the context.
     */
    public FunctionContext(final F function) {
        setFunction(function);
    }

    /**
     * @return {@link uk.gov.gchq.gaffer.function.Function} wrapped by this context.
     */
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public F getFunction() {
        return function;
    }

    /**
     * @param function {@link uk.gov.gchq.gaffer.function.Function} to be wrapped by this context.
     */
    public void setFunction(final F function) {
        this.function = function;
    }

    /**
     * Implementation of the Builder pattern for {@link uk.gov.gchq.gaffer.function.context.FunctionContext}.
     *
     * @param <F> The type of {@link uk.gov.gchq.gaffer.function.Function} wrapped by the context.
     */
    public abstract static class Builder<F extends Function> {
        private final FunctionContext<F> context;
        private boolean executed = false;

        /**
         * Create a <code>Builder</code> to configure a {@link uk.gov.gchq.gaffer.function.context.FunctionContext}.
         *
         * @param context {@link uk.gov.gchq.gaffer.function.context.FunctionContext} to be configured.
         */
        public Builder(final FunctionContext<F> context) {
            this.context = context;
        }

        /**
         * Sets the {@link uk.gov.gchq.gaffer.function.Function} to be wrapped by the context and returns this
         * <code>Builder</code> for further configuration.
         *
         * @param function {@link uk.gov.gchq.gaffer.function.Function} to be wrapped by the context.
         * @return This <code>Builder</code>.
         */
        public Builder<F> execute(final F function) {
            if (!executed) {
                context.setFunction(function);
                executed = true;
            } else {
                throw new IllegalStateException("Function has already been set");
            }

            return this;
        }

        /**
         * Tests whether the {@link uk.gov.gchq.gaffer.function.Function} to be wrapped has been configured.
         *
         * @return False until <code>aggregate(F)</code> is called, then true.
         */
        public boolean isExecuted() {
            return executed;
        }

        /**
         * Build the {@link uk.gov.gchq.gaffer.function.context.FunctionContext} configured by this <code>Builder</code>.
         *
         * @return Configured {@link uk.gov.gchq.gaffer.function.context.FunctionContext}
         */
        public FunctionContext<F> build() {
            return getContext();
        }

        /**
         * Get the {@link uk.gov.gchq.gaffer.function.context.FunctionContext} configured by this <code>Builder</code>. Equivalent
         * to <code>build()</code>.
         *
         * @return Configured {@link uk.gov.gchq.gaffer.function.context.FunctionContext}
         */
        public FunctionContext<F> getContext() {
            return context;
        }
    }
}
