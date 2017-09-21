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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.JsonNode;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationChainDAO;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A {@code AddNamedOperation} is an {@link Operation} for creating a new {@link NamedOperation}
 * and adding it to a Gaffer graph.
 */
public class AddNamedOperation implements Operation {
    private String operations = null;
    private String operationName;
    private String description;
    private List<String> readAccessRoles = new ArrayList<>();
    private List<String> writeAccessRoles = new ArrayList<>();
    private boolean overwriteFlag = false;
    private Map<String, ParameterDetail> parameters;
    private Map<String, String> options;

    private static final String CHARSET_NAME = CommonConstants.UTF_8;

    public boolean isOverwriteFlag() {
        return overwriteFlag;
    }

    public void setOverwriteFlag(final boolean overwriteFlag) {
        this.overwriteFlag = overwriteFlag;
    }

    public void setOperationChain(final String operationChain) {
        this.operations = operationChain;
    }

    @JsonSetter("operationChain")
    public void setOperationChain(final JsonNode opChainNode) {
        this.operations = opChainNode.toString();
    }

    @JsonIgnore
    public String getOperationChainAsString() {
        return operations;
    }

    @JsonGetter("operationChain")
    public JsonNode getOperationChainAsJsonNode() {
        try {
            return JSONSerialiser.getJsonNodeFromString(operations);
        } catch (final SerialisationException se) {
            throw new IllegalArgumentException(se.getMessage());
        }
    }

    public void setOperationChain(final OperationChain operationChain) {
        try {
            if (operationChain instanceof OperationChainDAO) {
                this.operations = new String(JSONSerialiser.serialise(operationChain), Charset.forName(CHARSET_NAME));
            } else {
                final OperationChainDAO dao = new OperationChainDAO(operationChain.getOperations());
                this.operations = new String(JSONSerialiser.serialise(dao), Charset.forName(CHARSET_NAME));
            }
        } catch (final SerialisationException se) {
            throw new IllegalArgumentException(se.getMessage());
        }
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

    public void setParameters(final Map<String, ParameterDetail> parameters) {
        this.parameters = parameters;
    }

    public Map<String, ParameterDetail> getParameters() {
        return parameters;
    }

    @Override
    public AddNamedOperation shallowClone() {
        return new AddNamedOperation.Builder()
                .operationChain(operations)
                .name(operationName)
                .description(description)
                .readAccessRoles(readAccessRoles.toArray(new String[readAccessRoles.size()]))
                .writeAccessRoles(writeAccessRoles.toArray(new String[writeAccessRoles.size()]))
                .overwrite(overwriteFlag)
                .parameters(parameters)
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

    public static class Builder extends BaseBuilder<AddNamedOperation, Builder> {
        public Builder() {
            super(new AddNamedOperation());
        }

        public Builder operationChain(final String opChainString) {
            _getOp().setOperationChain(opChainString);
            return _self();
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

        public Builder parameters(final Map<String, ParameterDetail> parameters) {
            _getOp().setParameters(parameters);
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
