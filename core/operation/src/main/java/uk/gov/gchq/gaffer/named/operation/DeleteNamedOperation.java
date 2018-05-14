/*
 * Copyright 2016-2018 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * A {@code DeleteNamedOperation} is an {@link Operation} for removing an existing
 * {@link NamedOperation} from a Gaffer graph.
 */
@JsonPropertyOrder(value = {"class", "operationName"}, alphabetic = true)
@Since("1.0.0")
@Summary("Deletes a named operation")
public class DeleteNamedOperation implements Operation {
    @Required
    private String operationName;
    private Map<String, String> options;

    public String getOperationName() {
        return operationName;
    }

    public void setOperationName(final String operationName) {
        this.operationName = operationName;
    }

    @Override
    public DeleteNamedOperation shallowClone() {
        return new DeleteNamedOperation.Builder()
                .name(operationName)
                .options(options)
                .build();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
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
