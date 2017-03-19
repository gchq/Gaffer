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
import uk.gov.gchq.gaffer.operation.AbstractOperation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AddNamedOperation extends AbstractOperation<Void, Void> {
    private OperationChain operationChain;
    private String operationName;
    private String description;
    private List<String> readAccessRoles = new ArrayList<>();
    private List<String> writeAccessRoles = new ArrayList<>();
    private boolean overwriteFlag = false;

    public boolean isOverwriteFlag() {
        return overwriteFlag;
    }

    public void setOverwriteFlag(final boolean overwriteFlag) {
        this.overwriteFlag = overwriteFlag;
    }

    public OperationChain getOperationChain() {
        return operationChain;
    }

    public void setOperationChain(final OperationChain operationChain) {
        this.operationChain = operationChain;
    }

    public String getOperationName() {
        return operationName;
    }

    public void setOperationName(final String operationName) {
        this.operationName = operationName;
    }

    public List<String> getReadAccessRoles() {
        return readAccessRoles;
    }

    public void setReadAccessRoles(final List<String> readAccessRoles) {
        this.readAccessRoles = readAccessRoles;
    }

    public List<String> getWriteAccessRoles() {
        return writeAccessRoles;
    }

    public void setWriteAccessRoles(final List<String> writeAccessRoles) {
        this.writeAccessRoles = writeAccessRoles;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    @Override
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.Void();
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends AbstractOperation.BaseBuilder<AddNamedOperation, Void, Void, CHILD_CLASS> {
        public BaseBuilder() {
            super(new AddNamedOperation());
        }

        public CHILD_CLASS operationChain(final OperationChain opChain) {
            getOp().setOperationChain(opChain);
            return self();
        }

        public CHILD_CLASS name(final String name) {
            getOp().setOperationName(name);
            return self();
        }

        public CHILD_CLASS description(final String description) {
            getOp().setDescription(description);
            return self();
        }

        public CHILD_CLASS readAccessRoles(final String... roles) {
            Collections.addAll(getOp().getReadAccessRoles(), roles);
            return self();
        }

        public CHILD_CLASS writeAccessRoles(final String... roles) {
            Collections.addAll(getOp().getWriteAccessRoles(), roles);
            return self();
        }

        public CHILD_CLASS overwrite(final boolean overwriteFlag) {
            getOp().setOverwriteFlag(overwriteFlag);
            return self();
        }

        public CHILD_CLASS overwrite() {
            return overwrite(true);
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}
