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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationChainDAO;
import uk.gov.gchq.gaffer.user.User;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Simple POJO containing the details associated with a {@link NamedOperation}.
 */
public class NamedOperationDetail implements Serializable {
    private static final long serialVersionUID = -8831783492657131469L;
    private static final String CHARSET_NAME = CommonConstants.UTF_8;
    private String operationName;
    private String description;
    private String creatorId;
    private String operations;
    private List<String> readAccessRoles;
    private List<String> writeAccessRoles;
    private Map<String, ParameterDetail> parameters = Maps.newHashMap();
    private Integer score;

    public NamedOperationDetail(final String operationName, final String description, final String userId,
                                final String operations, final List<String> readers,
                                final List<String> writers, final Map<String, ParameterDetail> parameters,
                                final Integer score) {
        if (null == operations) {
            throw new IllegalArgumentException("Operation Chain must not be empty");
        }
        if (null == operationName || operationName.isEmpty()) {
            throw new IllegalArgumentException("Operation Name must not be empty");
        }

        this.operationName = operationName;
        this.description = description;
        this.creatorId = userId;
        this.operations = operations;

        this.readAccessRoles = readers;
        this.writeAccessRoles = writers;
        this.parameters = parameters;
        this.score = score;
    }

    public String getOperationName() {
        return operationName;
    }

    public String getDescription() {
        return description;
    }

    public String getOperations() {
        return operations;
    }

    public List<String> getReadAccessRoles() {
        return readAccessRoles;
    }

    public List<String> getWriteAccessRoles() {
        return writeAccessRoles;
    }

    public String getCreatorId() {
        return creatorId;
    }

    public Map<String, ParameterDetail> getParameters() {
        return parameters;
    }

    public Integer getScore() {
        return score;
    }

    private String buildParamNameString(final String paramKey) {
        return "\"${" + paramKey + "}\"";
    }

    /**
     * Gets the OperationChain after adding in default values for any parameters. If a parameter
     * does not have a default, null is inserted.
     *
     * @return The {@link OperationChain}
     * @throws IllegalArgumentException if substituting the parameters fails
     */
    @JsonIgnore
    public OperationChain getOperationChainWithDefaultParams() {
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

        OperationChain opChain;
        try {
            opChain = JSONSerialiser.deserialise(opStringWithDefaults.getBytes(CHARSET_NAME), OperationChainDAO.class);
        } catch (final Exception e) {
            throw new IllegalArgumentException(e.getMessage());
        }

        return opChain;
    }

    /**
     * Gets the OperationChain after adding in any provided parameters.
     *
     * @param executionParams the parameters for the {@link uk.gov.gchq.gaffer.operation.Operation} to be executed
     * @return The {@link OperationChain}
     * @throws IllegalArgumentException if substituting the parameters fails
     */
    public OperationChain getOperationChain(final Map<String, Object> executionParams) {
        String opStringWithParams = operations;

        // First check all the parameters supplied are expected parameter names
        if (null != parameters) {
            if (null != executionParams) {
                Set<String> paramDetailKeys = parameters.keySet();
                Set<String> paramKeys = executionParams.keySet();

                if (!paramDetailKeys.containsAll(paramKeys)) {
                    throw new IllegalArgumentException("Unexpected parameter name in NamedOperation");
                }
            }

            for (final Map.Entry<String, ParameterDetail> parameterDetailPair : parameters.entrySet()) {
                String paramKey = parameterDetailPair.getKey();
                try {
                    if (null != executionParams && executionParams.containsKey(paramKey)) {
                        Object paramObj = JSONSerialiser.deserialise(JSONSerialiser.serialise(executionParams.get(paramKey)), parameterDetailPair.getValue().getValueClass());

                        opStringWithParams = opStringWithParams.replace(buildParamNameString(paramKey),
                                new String(JSONSerialiser.serialise(paramObj, CHARSET_NAME), CHARSET_NAME));
                    } else if (!parameterDetailPair.getValue().isRequired()) {
                        opStringWithParams = opStringWithParams.replace(buildParamNameString(paramKey),
                                new String(JSONSerialiser.serialise(parameterDetailPair.getValue().getDefaultValue(), CHARSET_NAME), CHARSET_NAME));
                    } else {
                        throw new IllegalArgumentException("Missing parameter " + paramKey + " with no default");
                    }
                } catch (final SerialisationException | UnsupportedEncodingException e) {
                    throw new IllegalArgumentException(e.getMessage());
                }
            }
        }

        OperationChain opChain;

        try {
            opChain = JSONSerialiser.deserialise(opStringWithParams.getBytes(CHARSET_NAME), OperationChainDAO.class);
        } catch (final Exception e) {
            throw new IllegalArgumentException(e.getMessage());
        }

        return opChain;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final NamedOperationDetail op = (NamedOperationDetail) obj;

        return new EqualsBuilder()
                .append(operationName, op.operationName)
                .append(creatorId, op.creatorId)
                .append(operations, op.operations)
                .append(readAccessRoles, op.readAccessRoles)
                .append(writeAccessRoles, op.writeAccessRoles)
                .append(parameters, op.parameters)
                .append(score, op.score)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(71, 3)
                .append(operationName)
                .append(creatorId)
                .append(operations)
                .append(readAccessRoles)
                .append(writeAccessRoles)
                .append(parameters)
                .append(score)
                .hashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("creatorId", creatorId)
                .append("operations", operations)
                .append("readAccessRoles", readAccessRoles)
                .append("writeAccessRoles", writeAccessRoles)
                .append("parameters", parameters)
                .append("score", score)
                .toString();
    }

    public boolean hasReadAccess(final User user) {
        return hasAccess(user, readAccessRoles);
    }

    public boolean hasWriteAccess(final User user) {
        return hasAccess(user, writeAccessRoles);
    }

    private boolean hasAccess(final User user, final List<String> roles) {
        if (null != roles) {
            for (final String role : roles) {
                if (user.getOpAuths().contains(role)) {
                    return true;
                }
            }
        }
        return user.getUserId().equals(creatorId);
    }

    public static final class Builder {
        private String operationName;
        private String description;
        private String creatorId;
        private String opChain;
        private List<String> readers;
        private List<String> writers;
        private Map<String, ParameterDetail> parameters;
        private Integer score;

        public Builder creatorId(final String creatorId) {
            this.creatorId = creatorId;
            return this;
        }

        public Builder operationName(final String operationName) {
            this.operationName = operationName;
            return this;
        }

        public Builder description(final String description) {
            this.description = description;
            return this;
        }

        public Builder operationChain(final String opChain) {

            this.opChain = opChain;
            return this;
        }

        public Builder operationChain(final OperationChain opChain) {
            try {
                this.opChain = new String(JSONSerialiser.serialise(opChain), Charset.forName(CHARSET_NAME));
            } catch (final SerialisationException se) {
                throw new IllegalArgumentException(se.getMessage());
            }

            return this;
        }


        public Builder parameters(final Map<String, ParameterDetail> parameters) {
            this.parameters = parameters;
            return this;
        }

        public Builder readers(final List<String> readers) {
            this.readers = readers;
            return this;
        }

        public Builder writers(final List<String> writers) {
            this.writers = writers;
            return this;
        }

        public Builder score(final Integer score) {
            this.score = score;
            return this;
        }

        public NamedOperationDetail build() {
            return new NamedOperationDetail(operationName, description, creatorId, opChain, readers, writers, parameters, score);
        }
    }
}
