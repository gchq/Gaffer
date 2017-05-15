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


import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AddNamedOperation implements Operation {
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

    public static class Builder extends Operation.BaseBuilder<AddNamedOperation, Builder> {
        public Builder() {
            super(new AddNamedOperation());
        }

        public Builder operationChain(final OperationChain opChain) {
            _getOp().setOperationChain(opChain);
            return _self();
        }

        public Builder name(final String name) {
            _getOp().setOperationName(name);
            return _self();
        }

        public Builder description(final String description) {
            _getOp().setDescription(description);
            return _self();
        }

        public Builder readAccessRoles(final String... roles) {
            Collections.addAll(_getOp().getReadAccessRoles(), roles);
            return _self();
        }

        public Builder writeAccessRoles(final String... roles) {
            Collections.addAll(_getOp().getWriteAccessRoles(), roles);
            return _self();
        }

        public Builder overwrite(final boolean overwriteFlag) {
            _getOp().setOverwriteFlag(overwriteFlag);
            return _self();
        }

        public Builder overwrite() {
            return overwrite(true);
        }
    }
}
