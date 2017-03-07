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

package uk.gov.gchq.gaffer.operation;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractOperation<I, O> implements Operation<I, O> {
    /**
     * The input for the operation.
     */
    private I input;

    private Map<String, String> options = new HashMap<>();
    private TypeReference<?> outputTypeReference = createOutputTypeReference();

    @Override
    public O castToOutputType(final Object result) {
        return (O) result;
    }

    @Override
    public I getInput() {
        return input;
    }

    @Override
    public void setInput(final I input) {
        this.input = input;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public void addOption(final String name, final String value) {
        this.options.put(name, value);
    }

    @Override
    public String getOption(final String name) {
        return this.options.get(name);
    }

    @JsonGetter("options")
    Map<String, String> getJsonOptions() {
        return options.isEmpty() ? null : options;
    }

    @JsonIgnore
    @Override
    public TypeReference<O> getOutputTypeReference() {
        return (TypeReference<O>) outputTypeReference;
    }

    @Override
    public void setOutputTypeReference(final TypeReference<?> outputTypeReference) {
        this.outputTypeReference = outputTypeReference;
    }

    protected abstract TypeReference createOutputTypeReference();

    public abstract static class BaseBuilder<OP_TYPE extends AbstractOperation<I, O>,
            I,
            O,
            CHILD_CLASS extends BaseBuilder<OP_TYPE, I, O, ?>> {
        protected OP_TYPE op;

        protected BaseBuilder(final OP_TYPE op) {
            this.op = op;
        }

        /**
         * Builds the operation and returns it.
         *
         * @return the built operation.
         */
        public OP_TYPE build() {
            return op;
        }

        /**
         * @param input the input to set on the operation
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Operation#setInput(Object)
         */
        public CHILD_CLASS input(final I input) {
            op.setInput(input);
            return self();
        }

        /**
         * @param name  the name of the option to add
         * @param value the value of the option to add
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Operation#addOption(String, String)
         */
        public CHILD_CLASS option(final String name, final String value) {
            op.addOption(name, value);
            return self();
        }

        public CHILD_CLASS outputType(final TypeReference<?> typeReference) {
            op.setOutputTypeReference(typeReference);
            return self();
        }

        protected abstract CHILD_CLASS self();

        protected OP_TYPE getOp() {
            return op;
        }
    }
}
