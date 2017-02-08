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


import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.user.User;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

public class ExtendedNamedOperation extends NamedOperation {

    private static final long serialVersionUID = -8831783492657131469L;
    private static final String CHARSET_NAME = "UTF-8";
    private String creatorId;
    private static final JSONSerialiser SERIALISER = new JSONSerialiser();
    private String operations;
    private List<String> readAccessRoles;
    private List<String> writeAccessRoles;

    public ExtendedNamedOperation(@Nonnull final String operationName, final String description, final String userId, @Nonnull final OperationChain<?> operations, final List<String> readers, final List<String> writers) {
        super(operationName, description);
        this.creatorId = userId;
        try {
            this.operations = new String(SERIALISER.serialise(operations), Charset.forName(CHARSET_NAME));
        } catch (SerialisationException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        this.readAccessRoles = readers;
        this.writeAccessRoles = writers;
    }

    public ExtendedNamedOperation() {
        super();
    }

    public OperationChain<?> getOperationChain() {
        try {
            return SERIALISER.deserialise(operations.getBytes(Charset.forName(CHARSET_NAME)), OperationChain.class);
        } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    public NamedOperation getBasic() {
        return new NamedOperation(getOperationName(), getDescription());
    }

    public String getOperations() {
        return operations;
    }

    public void setOperations(final String operations) {
        this.operations = operations;
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

    public String getCreatorId() {
        return creatorId;
    }

    public void setCreatorId(final String creatorId) {
        this.creatorId = creatorId;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ExtendedNamedOperation that = (ExtendedNamedOperation) o;

        if (creatorId != null ? !creatorId.equals(that.creatorId) : that.creatorId != null) {
            return false;
        }
        if (operations != null ? !operations.equals(that.operations) : that.operations != null) {
            return false;
        }
        if (readAccessRoles != null ? !readAccessRoles.equals(that.readAccessRoles) : that.readAccessRoles != null) {
            return false;
        }
        if (writeAccessRoles != null ? !writeAccessRoles.equals(that.writeAccessRoles) : that.writeAccessRoles != null) {
            return false;
        }
        return super.equals(getBasic());


    }

    @Override
    public int hashCode() {
        int result = creatorId != null ? creatorId.hashCode() : 0;
        result = 31 * result + (operations != null ? operations.hashCode() : 0);
        result = 31 * result + (readAccessRoles != null ? readAccessRoles.hashCode() : 0);
        result = 31 * result + (writeAccessRoles != null ? writeAccessRoles.hashCode() : 0);
        result += super.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ExtendedNamedOperation{" +
                "operationName='" + getOperationName() + '\'' +
                ", description='" + getDescription() + '\'' +
                ", creatorId=" + creatorId +
                ", operations='" + operations + '\'' +
                ", readAccessRoles=" + readAccessRoles +
                ", writeAccessRoles=" + writeAccessRoles +
                '}';
    }

    public boolean hasReadAccess(@Nonnull final User user) {
        return hasAccess(user, readAccessRoles);
    }

    private boolean hasAccess(@Nonnull final User user, final List<String> roles) {
        for (final String role: roles) {
            if (user.getOpAuths().contains(role)) {
                return true;
            }
        }
        return user.getUserId().equals(creatorId);
    }

    public boolean hasWriteAccess(@Nonnull final User user) {
        return hasAccess(user, writeAccessRoles);
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

        public ExtendedNamedOperation build() {
            if (opChain == null || opChain.getOperations().isEmpty()) {
                throw new IllegalArgumentException("Operation Chain must not be empty");
            }
            if (operationName == null || operationName.equals("")) {
                throw new IllegalArgumentException("Operation Name must not be empty");
            }
            return new ExtendedNamedOperation(operationName, description, creatorId, opChain, readers, writers);
        }
    }

}
