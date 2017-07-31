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

package uk.gov.gchq.gaffer.named.operation;


import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.Map;

/**
 * Named operations enable encapsulation of an OperationChain into a new single <code>NamedOperation</code>.
 * The <code>NamedOperation</code> can be added to OperationChains and executed, just like any other Operation.
 * When run it executes the encapsulated OperationChain.
 * There are various possible uses for NamedOperations, including:
 * <ul>
 * <li>making it simpler to run frequently used OperationChains</li>
 * <li>in a controlled way, allowing specific OperationChains to be run by a user that would not normally have permission to run them</li>
 * </ul>
 * <p>
 * Named operations must take an iterable as an input but can produce any type
 * of output.
 *
 * @param <I_ITEM> the input iterable item type
 * @param <O>      the output type
 */
public class NamedOperation<I_ITEM, O> implements
        InputOutput<Iterable<? extends I_ITEM>, O>,
        MultiInput<I_ITEM> {
    private Iterable<? extends I_ITEM> input;

    @Required
    private String operationName;
    private Map<String, Object> parameters;

    @Override
    public Iterable<? extends I_ITEM> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends I_ITEM> input) {
        this.input = input;
    }

    public void setParameters(final Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public String getOperationName() {
        return operationName;
    }

    public void setOperationName(final String operationName) {
        this.operationName = operationName;
    }

    @Override
    public TypeReference<O> getOutputTypeReference() {
        return (TypeReference) new TypeReferenceImpl.Object();
    }

    public static class Builder<I_ITEM, O> extends BaseBuilder<NamedOperation<I_ITEM, O>, Builder<I_ITEM, O>>
            implements InputOutput.Builder<NamedOperation<I_ITEM, O>, Iterable<? extends I_ITEM>, O, Builder<I_ITEM, O>>,
            MultiInput.Builder<NamedOperation<I_ITEM, O>, I_ITEM, Builder<I_ITEM, O>> {
        public Builder() {
            super(new NamedOperation<>());
        }

        public Builder<I_ITEM, O> name(final String name) {
            _getOp().setOperationName(name);
            return _self();
        }

        public Builder<I_ITEM, O> parameters(final Map<String, Object> params) {
            _getOp().setParameters(params);
            return _self();
        }
    }
}
