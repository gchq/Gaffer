/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.query.impl;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.query.IBuild;
import uk.gov.gchq.gaffer.operation.query.IElementDirection;
import uk.gov.gchq.gaffer.operation.query.ISubOperationChain;

import java.util.List;

import static java.util.Objects.nonNull;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = As.EXISTING_PROPERTY, property = "class", defaultImpl = OperationChain.class)
public class QueryChain<OUT> extends OperationChain<OUT> implements ISubOperationChain<OUT>, IBuild {
    protected BaseBuilder getCurrentOperationBuilder() {
        return currentOperationBuilder;
    }

    private BaseBuilder currentOperationBuilder;

    private void clearFields() {
        currentOperationBuilder = null;
    }

    public QueryChain(final OperationChain<OUT> outOperationChain) {
        if (outOperationChain instanceof QueryChain) {
            QueryChain that = (QueryChain) outOperationChain;
            this.currentOperationBuilder = that.currentOperationBuilder;
        }
    }

    @JsonGetter(value = "class")
    Class<OperationChain> clazz() {
        return OperationChain.class;
    }

    public IElementDirection getElements(final Object... seed) {
        tidyUp();

        currentOperationBuilder = new GetElements.Builder().input(seed);

        return new ElementDirection<>(this);
    }

    protected void tidyUp() {
        if (nonNull(currentOperationBuilder)) {
            final Operation operation = currentOperationBuilder.build();

            addOperation(operation);
        }
    }

    private void addOperation(final Operation op) {
        final List<Operation> operations = this.getOperations();
        operations.add(op);
        super.setOperationArray(operations.toArray(new Operation[operations.size()]));
        clearFields();
    }

    @Override
    public QueryChain<OUT> nextOperation() {
        tidyUp();
        return this;
    }

    @Override
    public OperationChain build() {
        tidyUp();
        return this;
    }
}
