/*
 * Copyright 2016-2020 Crown Copyright
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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.access.AccessControlledResource;
import uk.gov.gchq.gaffer.access.ResourceType;
import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
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
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonDeserialize(builder = NamedOperationDetail.Builder.class)
public class NamedOperationDetail implements AccessControlledResource, Serializable {

    private static final long serialVersionUID = -8831783492657131469L;
    private static final String CHARSET_NAME = CommonConstants.UTF_8;
    private String operationName;
    private List<String> labels;
    private String inputType;
    private String description;
    private String creatorId;
    private String operations;
    private List<String> readAccessRoles;
    private List<String> writeAccessRoles;
    private Map<String, ParameterDetail> parameters = Maps.newHashMap();
    private Integer score;
    private String readAccessPredicateJson;
    private String writeAccessPredicateJson;

    public NamedOperationDetail() {
    }


    public NamedOperationDetail(final String operationName, final String description, final String userId,
                                final String operations, final List<String> readers,
                                final List<String> writers, final Map<String, ParameterDetail> parameters,
                                final Integer score) {
        this(operationName, null, null, description, userId, operations, readers, writers, parameters, score);
    }

    public NamedOperationDetail(final String operationName, final String inputType, final String description, final String userId,
                                final String operations, final List<String> readers,
                                final List<String> writers, final Map<String, ParameterDetail> parameters,
                                final Integer score) {
        this(operationName, null, inputType, description, userId, operations, readers, writers, parameters, score);
    }

    public NamedOperationDetail(final String operationName, final List<String> labels, final String inputType, final String description,
                                final String userId, final String operations, final List<String> readers,
                                final List<String> writers, final Map<String, ParameterDetail> parameters,
                                final Integer score) {
        this(operationName, labels, inputType, description, userId, operations, readers, writers, parameters, score, null, null);
    }

    public NamedOperationDetail(final String operationName, final List<String> labels, final String inputType, final String description,
                                final String userId, final String operations, final List<String> readers,
                                final List<String> writers, final Map<String, ParameterDetail> parameters,
                                final Integer score, final AccessPredicate readAccessPredicate, final AccessPredicate writeAccessPredicate) {
        if (null == operations) {
            throw new IllegalArgumentException("Operation Chain must not be empty");
        }
        if (null == operationName || operationName.isEmpty()) {
            throw new IllegalArgumentException("Operation Name must not be empty");
        }
        if (readers != null && readAccessPredicate != null) {
            throw new IllegalArgumentException("Only one of readers or readAccessPredicate should be supplied.");
        }
        if (writers != null && writeAccessPredicate != null) {
            throw new IllegalArgumentException("Only one of writers or writeAccessPredicate should be supplied.");
        }

        this.operationName = operationName;
        this.labels = labels;
        this.inputType = inputType;
        this.description = description;
        this.creatorId = userId;
        this.operations = operations;

        this.readAccessRoles = readers;
        this.writeAccessRoles = writers;
        this.parameters = parameters;
        this.score = score;

        try {
            this.readAccessPredicateJson = readAccessPredicate != null ? new String(JSONSerialiser.serialise(readAccessPredicate)) : null;
            this.writeAccessPredicateJson = writeAccessPredicate != null ? new String(JSONSerialiser.serialise(writeAccessPredicate)) : null;
        } catch (final SerialisationException e) {
            throw new IllegalArgumentException("Read and Write Access predicates must be json serialisable", e);
        }
    }

    public String getOperationName() {
        return operationName;
    }

    public List<String> getLabels() {
        return labels;
    }

    public String getInputType() {
        return inputType;
    }

    public void setInputType(final String inputType) {
        this.inputType = inputType;
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
                .append(labels, op.labels)
                .append(inputType, op.inputType)
                .append(creatorId, op.creatorId)
                .append(operations, op.operations)
                .append(readAccessRoles, op.readAccessRoles)
                .append(writeAccessRoles, op.writeAccessRoles)
                .append(parameters, op.parameters)
                .append(score, op.score)
                .append(readAccessPredicateJson, op.readAccessPredicateJson)
                .append(writeAccessPredicateJson, op.writeAccessPredicateJson)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(71, 3)
                .append(operationName)
                .append(inputType)
                .append(creatorId)
                .append(operations)
                .append(readAccessRoles)
                .append(writeAccessRoles)
                .append(parameters)
                .append(score)
                .append(readAccessPredicateJson)
                .append(writeAccessPredicateJson)
                .hashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("labels", labels)
                .append("inputType", inputType)
                .append("creatorId", creatorId)
                .append("operations", operations)
                .append("readAccessRoles", readAccessRoles)
                .append("writeAccessRoles", writeAccessRoles)
                .append("parameters", parameters)
                .append("score", score)
                .append("readAccessPredicate", readAccessPredicateJson)
                .append("writeAccessPredicate", writeAccessPredicateJson)
                .toString();
    }

    @Override
    public ResourceType getResourceType() {
        return ResourceType.NamedOperation;
    }

    public boolean hasReadAccess(final User user, final String adminAuth) {
        return getOrDefaultReadAccessPredicate().test(user, adminAuth);
    }

    public boolean hasWriteAccess(final User user, final String adminAuth) {
        return getOrDefaultWriteAccessPredicate().test(user, adminAuth);
    }

    public AccessPredicate getReadAccessPredicate() {
        try {
            return readAccessPredicateJson != null ? JSONSerialiser.deserialise(readAccessPredicateJson, AccessPredicate.class) : null;
        } catch (final SerialisationException e) {
            throw new IllegalArgumentException("readAccessPredicate was not JsonSerialisable", e);
        }
    }

    public AccessPredicate getWriteAccessPredicate() {
        try {
            return writeAccessPredicateJson != null ? JSONSerialiser.deserialise(writeAccessPredicateJson, AccessPredicate.class) : null;
        } catch (final SerialisationException e) {
            throw new IllegalArgumentException("writeAccessPredicate was not JsonSerialisable", e);
        }
    }

    @JsonIgnore
    public AccessPredicate getOrDefaultReadAccessPredicate() {
        final AccessPredicate readAccessPredicate = getReadAccessPredicate();
        return readAccessPredicate != null ? readAccessPredicate : getDefaultReadAccessPredicate();
    }

    @JsonIgnore
    public AccessPredicate getOrDefaultWriteAccessPredicate() {
        final AccessPredicate writeAccessPredicate = getWriteAccessPredicate();
        return writeAccessPredicate != null ? writeAccessPredicate : getDefaultWriteAccessPredicate();
    }

    private AccessPredicate getDefaultReadAccessPredicate() {
        return new AccessPredicate(this.creatorId, this.readAccessRoles);
    }

    private AccessPredicate getDefaultWriteAccessPredicate() {
        return new AccessPredicate(this.creatorId, this.writeAccessRoles);
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static final class Builder {
        private String operationName;
        private List<String> labels;
        private String inputType;
        private String description;
        private String creatorId;
        private String opChain;
        private List<String> readers;
        private List<String> writers;
        private Map<String, ParameterDetail> parameters = Maps.newHashMap();
        private Integer score;
        private AccessPredicate readAccessPredicate;
        private AccessPredicate writeAccessPredicate;

        public Builder creatorId(final String creatorId) {
            this.creatorId = creatorId;
            return this;
        }

        public Builder operationName(final String operationName) {
            this.operationName = operationName;
            return this;
        }

        public Builder labels(final List<String> labels) {
            this.labels = labels;
            return this;
        }

        public Builder inputType(final String inputType) {
            this.inputType = inputType;
            return this;
        }

        public Builder description(final String description) {
            this.description = description;
            return this;
        }

        @JsonProperty("operations")
        public Builder operationChain(final String opChain) {
            this.opChain = opChain;
            return this;
        }

        @JsonProperty("operationChain")
        public Builder operationChain(final OperationChain opChain) {
            this.opChain = serialise(opChain);
            return this;
        }

        public Builder parameters(final Map<String, ParameterDetail> parameters) {
            this.parameters = parameters;
            return this;
        }

        @JsonProperty("readAccessRoles")
        public Builder readers(final List<String> readers) {
            this.readers = readers;
            return this;
        }

        @JsonProperty("writeAccessRoles")
        public Builder writers(final List<String> writers) {
            this.writers = writers;
            return this;
        }

        public Builder score(final Integer score) {
            this.score = score;
            return this;
        }

        public Builder readAccessPredicate(final AccessPredicate readAccessPredicate) {
            this.readAccessPredicate = readAccessPredicate;
            return this;
        }

        public Builder writeAccessPredicate(final AccessPredicate writeAccessPredicate) {
            this.writeAccessPredicate = writeAccessPredicate;
            return this;
        }

        public NamedOperationDetail build() {
            return new NamedOperationDetail(operationName, labels, inputType, description, creatorId, opChain, readers, writers, parameters, score, readAccessPredicate, writeAccessPredicate);
        }

        private String serialise(final Object pojo) {
            try {
                return new String(JSONSerialiser.serialise(pojo), Charset.forName(CHARSET_NAME));
            } catch (final SerialisationException se) {
                throw new IllegalArgumentException(se.getMessage());
            }
        }
    }
}
