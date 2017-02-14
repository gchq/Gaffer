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
import uk.gov.gchq.gaffer.function.TransformFunction;
import uk.gov.gchq.gaffer.function.Tuple;
import uk.gov.gchq.gaffer.function.context.ConsumerProducerFunctionContext;
import java.util.ArrayList;
import java.util.List;

/**
 * A <code>Transformer</code> is a {@link uk.gov.gchq.gaffer.function.processor.Processor} that applies
 * {@link uk.gov.gchq.gaffer.function.TransformFunction}s to {@link uk.gov.gchq.gaffer.function.Tuple}s, changing their contents in some
 * way, whether that be modification of existing values or creation of new ones.
 *
 * @param <R> The type of reference used by tuples.
 */
public class Transformer<R> extends Processor<R, ConsumerProducerFunctionContext<R, TransformFunction>> {
    /**
     * Transform an input {@link uk.gov.gchq.gaffer.function.Tuple} using {@link uk.gov.gchq.gaffer.function.TransformFunction}s.
     *
     * @param tuple {@link uk.gov.gchq.gaffer.function.Tuple} to be transformed.
     */
    public void transform(final Tuple<R> tuple) {
        if (functions == null) {
            return;
        }
        for (final ConsumerProducerFunctionContext<R, TransformFunction> functionContext : functions) {
            TransformFunction function = functionContext.getFunction();
            Object[] selection = functionContext.select(tuple);
            Object[] result = function.transform(selection);
            functionContext.project(tuple, result);
        }
    }

    /**
     * Create a deep copy of this <code>Transformer</code>.
     *
     * @return Deep copy of this <code>Transformer</code>.
     */
    @SuppressWarnings("CloneDoesntCallSuperClone")
    @SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL", justification = "Does not required any fields from the parent class")
    @Override
    public Transformer<R> clone() {
        Transformer<R> clone = new Transformer<>();
        if (null != functions) {
            clone.addFunctions(cloneFunctions());
        }

        return clone;
    }

    /**
     * Create a deep copy of the {@link uk.gov.gchq.gaffer.function.context.ConsumerProducerFunctionContext}s executed by this
     * <code>Transformer</code>.
     *
     * @return Deep copy of {@link uk.gov.gchq.gaffer.function.context.ConsumerProducerFunctionContext}s.
     */
    protected List<ConsumerProducerFunctionContext<R, TransformFunction>> cloneFunctions() {
        final List<ConsumerProducerFunctionContext<R, TransformFunction>> functionClones = new ArrayList<>();

        for (final ConsumerProducerFunctionContext<R, TransformFunction> function : functions) {
            ConsumerProducerFunctionContext<R, TransformFunction> cloneContext = new ConsumerProducerFunctionContext<>();
            cloneContext.setSelection(function.getSelection());
            cloneContext.setProjection(function.getProjection());
            TransformFunction af = function.getFunction();
            if (af != null) {
                cloneContext.setFunction(af.statelessClone());
            }
            functionClones.add(cloneContext);
        }

        return functionClones;
    }

    /**
     * Implementation of the Builder pattern for {@link uk.gov.gchq.gaffer.function.processor.Transformer}.
     *
     * @param <R> The type of reference used by tuples.
     */
    public static class Builder<R> {
        private final Transformer<R> transformer;
        private ConsumerProducerFunctionContext.Builder<R, TransformFunction> contextBuilder = new ConsumerProducerFunctionContext.Builder<>();

        /**
         * Create a <code>Builder</code> for a new {@link uk.gov.gchq.gaffer.function.processor.Transformer}.
         */
        public Builder() {
            this(new Transformer<R>());
        }

        /**
         * Create a <code>Builder</code> for a {@link uk.gov.gchq.gaffer.function.processor.Transformer}.
         *
         * @param transformer {@link uk.gov.gchq.gaffer.function.processor.Transformer} to be configured.
         */
        public Builder(final Transformer<R> transformer) {
            this.transformer = transformer;
        }

        /**
         * Set the selection references for the current
         * {@link uk.gov.gchq.gaffer.function.context.ConsumerProducerFunctionContext}. If the current context already has a
         * selection set, a new context is created and made current. Returns this <code>Builder</code> for further
         * configuration.
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
         * Set the {@link uk.gov.gchq.gaffer.function.TransformFunction} to be executed by the current
         * {@link uk.gov.gchq.gaffer.function.context.ConsumerProducerFunctionContext}. If the current context already has a
         * function set, a new context is created and made current. Returns this <code>Builder</code> for further
         * configuration.
         *
         * @param function {@link uk.gov.gchq.gaffer.function.TransformFunction} to be executed.
         * @return This <code>Builder</code>.
         */
        public Builder<R> execute(final TransformFunction function) {
            if (contextBuilder.isExecuted()) {
                buildContext();
            }

            contextBuilder.execute(function);
            return this;
        }

        /**
         * Set the projection references for the current
         * {@link uk.gov.gchq.gaffer.function.context.ConsumerProducerFunctionContext}. If the current context already has a
         * projection set, a new context is created and made current. Returns this <code>Builder</code> for further
         * configuration.
         *
         * @param projection Projection references.
         * @return This <code>Builder</code>.
         */
        public Builder<R> project(final R... projection) {
            if (contextBuilder.isProjected()) {
                buildContext();
            }

            contextBuilder.project(projection);
            return this;
        }

        /**
         * Build the {@link uk.gov.gchq.gaffer.function.processor.Transformer} configured by this <code>Builder</code>.
         *
         * @return Configured {@link uk.gov.gchq.gaffer.function.processor.Transformer}.
         */
        public Transformer<R> build() {
            buildContext();
            return transformer;
        }

        /**
         * Build the current {@link uk.gov.gchq.gaffer.function.context.ConsumerProducerFunctionContext}, add it to the
         * {@link uk.gov.gchq.gaffer.function.processor.Transformer} and create a new current context.
         *
         * @return This <code>Builder</code>.
         */
        private Builder<R> buildContext() {
            if (contextBuilder.isSelected() || contextBuilder.isProjected() || contextBuilder.isExecuted()) {
                transformer.addFunction(contextBuilder.build());
                contextBuilder = new ConsumerProducerFunctionContext.Builder<>();
            }

            return this;
        }
    }
}
