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


import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;

public class DeleteNamedOperation implements Operation {
    @Required
    private String operationName;

    public String getOperationName() {
        return operationName;
    }

    public void setOperationName(final String operationName) {
        this.operationName = operationName;
    }

    public static class Builder extends BaseBuilder<DeleteNamedOperation, Builder> {
        public Builder() {
            super(new DeleteNamedOperation());
        }

        public Builder name(final String name) {
            _getOp().setOperationName(name);
            return _self();
        }
    }
}
