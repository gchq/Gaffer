/*
 * Copyright 2016-2019 Crown Copyright
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
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
<<<<<<< HEAD
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
=======
>>>>>>> parent of 8ae5ffa172... gh-2157 generated equals/hashcode and fixed erroring tests

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationChainDAO;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * A {@code AddNamedOperation} is an {@link Operation} for creating a new {@link NamedOperation}
 * and adding it to a Gaffer graph.
 */
@JsonPropertyOrder(value = {"class", "operationName", "description", "score", "operations"}, alphabetic = true)
@Since("1.0.0")
@Summary("Adds a new named operation")
public class AddNamedOperation implements Operation, Operations<Operation> {
    @Required
    private String operations;
    private String operationName;
    private String description;
    private List<String> readAccessRoles = new ArrayList<>();
    private List<String> writeAccessRoles = new ArrayList<>();
    private boolean overwriteFlag = false;
    private Map<String, ParameterDetail> parameters;
    private Map<String, String> options;
    private Integer score;

    private static final String CHARSET_NAME = CommonConstants.UTF_8;

    public boolean isOverwriteFlag() {
        return overwriteFlag;
    }

    public void setOverwriteFlag(final boolean overwriteFlag) {
        this.overwriteFlag = overwriteFlag;
    }

    @JsonIgnore
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

    @JsonIgnore
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
                .score(score)
                .build();
    }

    @Override
<<<<<<< HEAD
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final AddNamedOperation that = (AddNamedOperation) o;

        return new EqualsBuilder()
                .append(overwriteFlag, that.overwriteFlag)
                .append(operations, that.operations)
                .append(operationName, that.operationName)
                .append(description, that.description)
                .append(readAccessRoles, that.readAccessRoles)
                .append(writeAccessRoles, that.writeAccessRoles)
                .append(parameters, that.parameters)
                .append(options, that.options)
                .append(score, that.score)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(operations)
                .append(operationName)
                .append(description)
                .append(readAccessRoles)
                .append(writeAccessRoles)
                .append(overwriteFlag)
                .append(parameters)
                .append(options)
                .append(score)
                .toHashCode();
    }

    @Override
=======
>>>>>>> parent of 8ae5ffa172... gh-2157 generated equals/hashcode and fixed erroring tests
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(final Integer score) {
        this.score = score;
    }

    /**
     * @return a list of the operations in the operation chain resolved using the default parameters.
     */
    @Override
    @JsonIgnore
    public Collection<Operation> getOperations() {
        return getOperationsWithDefaultParams();
    }

    @Override
    public void updateOperations(final Collection<Operation> operations) {
        // ignore - Named operations will be updated when run instead
    }

    private Collection<Operation> getOperationsWithDefaultParams() {
        String opStringWithDefaults = operations;

        if (null != parameters) {
            for (final Map.Entry<String, ParameterDetail> parameterDetailPair : parameters.entrySet()) {
                String paramKey = parameterDetailPair.getKey();

                try {
                    opStringWithDefaults = opStringWithDefaults.replace(buildParamNameString(paramKey),
                            new String(JSONSerialiser.serialise(parameterDetailPair.getValue().getDefaultValue(), CHARSET_NAME), CHARSET_NAME));
                } catch (final SerialisationException | UnsupportedEncodingException e) {
                    throw new IllegalArgumentException(e.getMessage());
                }
            }
        }

        OperationChain<?> opChain;
        if (StringUtils.isEmpty(opStringWithDefaults)) {
            opChain = null;
        } else {
            try {
                opChain = JSONSerialiser.deserialise(opStringWithDefaults.getBytes(CHARSET_NAME), OperationChainDAO.class);
            } catch (final Exception e) {
                opChain = null;
            }
        }

        final List<Operation> operations = new ArrayList<>();
        if (nonNull(opChain) && nonNull(opChain.getOperations())) {
            operations.addAll(opChain.getOperations());
        }
        return operations;
    }

    private String buildParamNameString(final String paramKey) {
        return "\"${" + paramKey + "}\"";
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

        public Builder parameter(final String name, final ParameterDetail detail) {
            Map<String, ParameterDetail> parameters = _getOp().getParameters();
            if (isNull(parameters)) {
                parameters = new HashMap<>();
                _getOp().setParameters(parameters);
            }
            parameters.put(name, detail);
            return _self();
        }

        public Builder overwrite(final boolean overwriteFlag) {
            _getOp().setOverwriteFlag(overwriteFlag);
            return _self();
        }

        public Builder overwrite() {
            return overwrite(true);
        }

        public Builder score(final Integer score) {
            _getOp().setScore(score);
            return _self();
        }
    }
}
