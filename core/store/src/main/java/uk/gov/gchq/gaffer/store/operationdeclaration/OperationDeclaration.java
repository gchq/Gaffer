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
package uk.gov.gchq.gaffer.store.operationdeclaration;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

/**
 * A single <code>OperationDeclaration</code> describes an operation handler.
 */
public class OperationDeclaration {
    private Class<? extends Operation> operation;

    private OperationHandler handler;

    public Class<? extends Operation> getOperation() {
        return operation;
    }

    public void setOperation(final Class<? extends Operation> operation) {
        this.operation = operation;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public OperationHandler getHandler() {
        return handler;
    }

    public void setHandler(final OperationHandler handler) {
        this.handler = handler;
    }

    @Override
    public String toString() {
        return "OperationDeclaration{"
                + "operation=" + operation
                + ", handler=" + handler
                + '}';
    }

    public static class Builder {
        private final OperationDeclaration instance;

        public Builder() {
            instance = new OperationDeclaration();
        }

        public Builder operation(final Class<? extends Operation> operation) {
            this.instance.setOperation(operation);
            return this;
        }

        public Builder handler(final OperationHandler handler) {
            this.instance.setHandler(handler);
            return this;
        }

        public OperationDeclaration build() {
            return this.instance;
        }
    }


}
