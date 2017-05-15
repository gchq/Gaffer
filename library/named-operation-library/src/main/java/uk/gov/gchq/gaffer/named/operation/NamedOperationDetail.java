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


import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;

public class NamedOperationDetail implements Serializable {
    private static final JSONSerialiser SERIALISER = new JSONSerialiser();
    private static final long serialVersionUID = -8831783492657131469L;
    private static final String CHARSET_NAME = "UTF-8";
    private String operationName;
    private String description;
    private String creatorId;
    private String operations;
    private List<String> readAccessRoles;
    private List<String> writeAccessRoles;

    public NamedOperationDetail(final String operationName, final String description, final String userId, final OperationChain<?> operations, final List<String> readers, final List<String> writers) {
        if (operations == null || null == operations.getOperations() || operations.getOperations().isEmpty()) {
            throw new IllegalArgumentException("Operation Chain must not be empty");
        }
        if (operationName == null || operationName.isEmpty()) {
            throw new IllegalArgumentException("Operation Name must not be empty");
        }

        this.operationName = operationName;
        this.description = description;
        this.creatorId = userId;
        try {
            this.operations = new String(SERIALISER.serialise(operations), Charset.forName(CHARSET_NAME));
        } catch (final SerialisationException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        this.readAccessRoles = readers;
        this.writeAccessRoles = writers;
    }

    public OperationChain<?> getOperationChain() {
        try {
            return SERIALISER.deserialise(operations.getBytes(Charset.forName(CHARSET_NAME)), OperationChain.class);
        } catch (final IOException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final NamedOperationDetail op = (NamedOperationDetail) o;

        return new EqualsBuilder()
                .append(operationName, op.operationName)
                .append(creatorId, op.creatorId)
                .append(operations, op.operations)
                .append(readAccessRoles, op.readAccessRoles)
                .append(writeAccessRoles, op.writeAccessRoles)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(operationName)
                .append(creatorId)
                .append(operations)
                .append(readAccessRoles)
                .append(writeAccessRoles)
                .hashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("creatorId", creatorId)
                .append("creatorId", operations)
                .append("readAccessRoles", readAccessRoles)
                .append("writeAccessRoles", writeAccessRoles)
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
        private OperationChain<?> opChain;
        private List<String> readers;
        private List<String> writers;

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

        public Builder operationChain(final OperationChain<?> opChain) {
            this.opChain = opChain;
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

        public NamedOperationDetail build() {
            return new NamedOperationDetail(operationName, description, creatorId, opChain, readers, writers);
        }
    }

}
