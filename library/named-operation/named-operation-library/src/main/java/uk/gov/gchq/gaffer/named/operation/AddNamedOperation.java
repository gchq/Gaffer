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
import java.util.List;

public class AddNamedOperation extends AbstractOperation<Void, Void> {
    private OperationChain operationChain;
    private String operationName;
    private String description;
    private List<String> readAccessRoles = new ArrayList<>();
    private List<String> writeAccessRoles = new ArrayList<>();
    private boolean overwriteFlag = false;

    public AddNamedOperation() {
        super();
    }

    public AddNamedOperation(final boolean overwrite) {
        super();
        this.overwriteFlag = overwrite;
    }

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

}
