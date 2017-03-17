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
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.operation.io.IterableInputIterableOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.io.Serializable;

public class NamedOperation<I_ITEM, O_ITEM> implements
        IterableInputIterableOutput<I_ITEM, O_ITEM>,
        OperationView,
        Serializable {
    private static final long serialVersionUID = -356445124131310528L;
    private View view;
    private String operationName;
    private String description;
    private Iterable<I_ITEM> input;

    public NamedOperation() {
    }

    protected NamedOperation(final String operationName, final String description) {
        super();
        this.operationName = operationName;
        this.description = description;
    }

    @Override
    public View getView() {
        return view;
    }

    @Override
    public void setView(final View view) {
        this.view = view;
    }

    public String getOperationName() {
        return operationName;
    }

    public void setOperationName(final String operationName) {
        this.operationName = operationName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }


    @Override
    public Iterable<I_ITEM> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<I_ITEM> input) {
        this.input = input;
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final NamedOperation op = (NamedOperation) o;

        return new EqualsBuilder()
                .append(operationName, op.operationName)
                .append(description, op.description)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(operationName)
                .append(description)
                .hashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("operationName", operationName)
                .append("description", description)
                .toString();
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

        public Builder<I_ITEM, O_ITEM> description(final String description) {
            _getOp().setDescription(description);
            return _self();
        }
    }
}
