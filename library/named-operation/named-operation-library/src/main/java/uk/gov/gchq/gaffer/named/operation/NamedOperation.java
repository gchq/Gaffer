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
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.operation.io.IterableInputIterableOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

public class NamedOperation<I_ITEM, O_ITEM> implements
        IterableInputIterableOutput<I_ITEM, O_ITEM>,
        OperationView {
    private View view;
    private Iterable<I_ITEM> input;
    private String operationName;

    @Override
    public View getView() {
        return view;
    }

    @Override
    public void setView(final View view) {
        this.view = view;
    }

    @Override
    public Iterable<I_ITEM> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<I_ITEM> input) {
        this.input = input;
    }

    public String getOperationName() {
        return operationName;
    }

    public void setOperationName(final String operationName) {
        this.operationName = operationName;
    }

    @Override
    public TypeReference<CloseableIterable<O_ITEM>> getOutputTypeReference() {
        return (TypeReference) new TypeReferenceImpl.CloseableIterableObj();
    }

    public static class Builder<I_ITEM, O_ITEM> extends Operation.BaseBuilder<NamedOperation<I_ITEM, O_ITEM>, Builder<I_ITEM, O_ITEM>>
            implements IterableInputIterableOutput.Builder<NamedOperation<I_ITEM, O_ITEM>, I_ITEM, O_ITEM, Builder<I_ITEM, O_ITEM>> {
        public Builder() {
            super(new NamedOperation<>());
        }

        public Builder<I_ITEM, O_ITEM> name(final String name) {
            _getOp().setOperationName(name);
            return _self();
        }
    }
}
