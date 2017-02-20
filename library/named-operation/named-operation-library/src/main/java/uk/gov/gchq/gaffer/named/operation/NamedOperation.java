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
import uk.gov.gchq.gaffer.operation.AbstractGetOperation;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.io.Serializable;

public class NamedOperation extends AbstractGetOperation<Object, Object> implements Serializable {
    private static final long serialVersionUID = -356445124131310528L;
    private String operationName;
    private String description;

    public NamedOperation() {
        super();
    }

    public NamedOperation(final Iterable<Object> input) {
        super(input);
    }

    public NamedOperation(final CloseableIterable<Object> input) {
        super(input);
    }

    public NamedOperation(final View view) {
        super(view);
    }

    public NamedOperation(final CloseableIterable<Object> input, final View view) {
        super(view, input);
    }

    public NamedOperation(final String operationName, final String description) {
        super();
        this.operationName = operationName;
        this.description = description;
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
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.Object();
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends AbstractGetOperation.BaseBuilder<NamedOperation, Object, Object, CHILD_CLASS> {
        public BaseBuilder() {
            super(new NamedOperation());
        }

        public CHILD_CLASS name(final String name) {
            getOp().setOperationName(name);
            return self();
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}
