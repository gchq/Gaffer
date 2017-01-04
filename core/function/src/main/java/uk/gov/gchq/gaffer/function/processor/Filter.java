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

package uk.gov.gchq.gaffer.function.processor;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.function.FilterFunction;
import uk.gov.gchq.gaffer.function.Tuple;
import uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A <code>Filter</code> is a {@link uk.gov.gchq.gaffer.function.processor.Processor} that tests input
 * {@link uk.gov.gchq.gaffer.function.Tuple}s against {@link uk.gov.gchq.gaffer.function.FilterFunction}s. The functions are executed in
 * order and the overall filter result for a given tuple is a logical AND of each filter function result. Tuples only
 * pass the filter if <b>all</b> functions return a positive (<code>true</code>) result, and they fail as soon as any
 * function returns a negative (<code>false</code>) result.
 * <p>
 * If performance is a concern then simple, faster filters or those with a higher probability of failure should be
 * configured to run first.
 *
 * @param <R> The type of reference used by tuples.
 */
public class Filter<R> extends Processor<R, ConsumerFunctionContext<R, FilterFunction>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Filter.class);

    /**
     * Default constructor - used for serialisation.
     */
    public Filter() {
        super();
    }

    /**
     * Create a <code>Filter</code> that executes the given {@link uk.gov.gchq.gaffer.function.context.FunctionContext}s.
     *
     * @param functions {@link uk.gov.gchq.gaffer.function.context.FunctionContext}s to execute.
     */
    public Filter(final List<ConsumerFunctionContext<R, FilterFunction>> functions) {
        super(functions);
    }

    /**
     * Create a <code>Filter</code> that executes the given {@link uk.gov.gchq.gaffer.function.context.FunctionContext}s.
     *
     * @param functions {@link uk.gov.gchq.gaffer.function.context.FunctionContext}s to execute.
     */
    @SafeVarargs
    public Filter(final ConsumerFunctionContext<R, FilterFunction>... functions) {
        this(Arrays.asList(functions));
    }

    /**
     * Create a deep copy of this <code>Filter</code>.
     *
     * @return Deep copy of this <code>Filter</code>.
     */
    @SuppressWarnings("CloneDoesntCallSuperClone")
    @SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL", justification = "Does not required any fields from the parent class")
    @Override
    public Filter<R> clone() {
        Filter<R> clone = new Filter<>();
        if (null != functions) {
            clone.addFunctions(cloneFunctions());
        }

        return clone;
    }

    /**
     * Create a deep copy of the {@link uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext}s executed by this
     * <code>Filter</code>.
     *
     * @return Deep copy of {@link uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext}s.
     */
    protected List<ConsumerFunctionContext<R, FilterFunction>> cloneFunctions() {
        final List<ConsumerFunctionContext<R, FilterFunction>> functionClones = new ArrayList<>();

        for (final ConsumerFunctionContext<R, FilterFunction> function : functions) {
            final ConsumerFunctionContext<R, FilterFunction> cloneContext = new ConsumerFunctionContext<>();
            cloneContext.setSelection(function.getSelection());

            final FilterFunction af = function.getFunction();
            if (af != null) {
                cloneContext.setFunction(af.statelessClone());
            }
            functionClones.add(cloneContext);
        }

        return functionClones;
    }

    /**
     * Test an input {@link uk.gov.gchq.gaffer.function.Tuple} against {@link uk.gov.gchq.gaffer.function.FilterFunction}s, performing a
     * logical AND.
     *
     * @param tuple {@link uk.gov.gchq.gaffer.function.Tuple} to be filtered.
     * @return Logical AND of filter function results.
     */
    public boolean filter(final Tuple<R> tuple) {
        if (functions == null) {
            return true;
        }

        for (final ConsumerFunctionContext<R, FilterFunction> functionContext : functions) {
            FilterFunction function = functionContext.getFunction();
            Object[] selection = functionContext.select(tuple);
            boolean result = function.isValid(selection);

            if (!result) {
                LOGGER.debug(function.getClass().getName() + " filtered out "
                        + Arrays.toString(selection) + " from input: " + tuple);
                return false;
            }
        }

        return true;
    }



    /**
     * Implementation of the Builder pattern for {@link uk.gov.gchq.gaffer.function.processor.Filter}.
     *
     * @param <R> The type of reference used by tuples.
     */
    public static class Builder<R> {
        private final Filter<R> filter;
        private ConsumerFunctionContext.Builder<R, FilterFunction> contextBuilder = new ConsumerFunctionContext.Builder<>();

        /**
         * Create a <code>Builder</code> to configure a new {@link uk.gov.gchq.gaffer.function.processor.Filter}.
         */
        public Builder() {
            this(new Filter<R>());
        }

        /**
         * Create a <code>Builder</code> to configure a {@link uk.gov.gchq.gaffer.function.processor.Filter}.
         *
         * @param filter {@link uk.gov.gchq.gaffer.function.processor.Filter} to be configured.
         */
        public Builder(final Filter<R> filter) {
            this.filter = filter;
        }

        /**
         * Set the selection references for the current {@link uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext}. If
         * the current context already has a selection set, a new context is created and made current. Returns this
         * <code>Builder</code> for further configuration.
         *
         * @param selection Selection references.
         * @return This <code>Builder</code>.
         */
        public Builder<R> select(final R... selection) {
            if (contextBuilder.isSelected()) {
                buildContext();
            }

            contextBuilder.select(selection);
            return this;
        }

        /**
         * Set the {@link uk.gov.gchq.gaffer.function.FilterFunction} to be executed by the current
         * {@link uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext}. If the current context already has a function set,
         * a new context is created and made current. Returns this <code>Builder</code> for further configuration.
         *
         * @param function {@link uk.gov.gchq.gaffer.function.FilterFunction} to be executed.
         * @return This <code>Builder</code>.
         */
        public Builder<R> execute(final FilterFunction function) {
            if (contextBuilder.isExecuted()) {
                buildContext();
            }

            contextBuilder.execute(function);
            return this;
        }

        /**
         * Build the {@link uk.gov.gchq.gaffer.function.processor.Filter} configured by this <code>Builder</code>.
         *
         * @return Configured {@link uk.gov.gchq.gaffer.function.processor.Filter}.
         */
        public Filter<R> build() {
            buildContext();
            return filter;
        }

        /**
         * Build the current {@link uk.gov.gchq.gaffer.function.context.ConsumerFunctionContext}, add it to the
         * {@link uk.gov.gchq.gaffer.function.processor.Filter} and create a new current context.
         *
         * @return This <code>Builder</code>.
         */
        private Builder<R> buildContext() {
            if (contextBuilder.isSelected() || contextBuilder.isExecuted()) {
                filter.addFunction(contextBuilder.build());
                contextBuilder = new ConsumerFunctionContext.Builder<>();
            }

            return this;
        }
    }
}
