/*
 * Copyright 2017-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import uk.gov.gchq.gaffer.access.AccessControlledResource;
import uk.gov.gchq.gaffer.access.ResourceType;
import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.federatedstore.access.predicate.FederatedGraphReadAccessPredicate;
import uk.gov.gchq.gaffer.federatedstore.access.predicate.FederatedGraphWriteAccessPredicate;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.user.User;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.isNull;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.DEFAULT_VALUE_IS_PUBLIC;

/**
 * Conditions required for a {@link User} to have access to a graph within the
 * {@link FederatedStore} via {@link FederatedAccess}
 * <table>
 * <caption>FederatedAccess truth table</caption>
 * <tr><td> User Ops</td><td> AccessHook Ops</td><td> User added graph
 * </td><td> hasAccess?</td></tr>
 * <tr><td> 'A'     </td><td> 'A'           </td><td> n/a
 * </td><td> T         </td></tr>
 * <tr><td> 'A','B' </td><td> 'A'           </td><td> n/a
 * </td><td> T         </td></tr>
 * <tr><td> 'A'     </td><td> 'A','B'       </td><td> n/a
 * </td><td> T         </td></tr>
 * <tr><td> 'A'     </td><td> 'B'           </td><td> F
 * </td><td> F         </td></tr>
 * <tr><td> 'A'     </td><td> 'B'           </td><td> T
 * </td><td> T         </td></tr>
 * <tr><td> n/a     </td><td> {@code null}  </td><td> T
 * </td><td> T         </td></tr>
 * <tr><td> n/a     </td><td> {@code null}  </td><td> F
 * </td><td> F         </td></tr>
 * <tr><td> n/a     </td><td> {@code empty} </td><td> T
 * </td><td> T         </td></tr>
 * <tr><td> n/a     </td><td> {@code empty} </td><td> F
 * </td><td> F         </td></tr>
 * </table>
 * @deprecated Will be replaced by a GraphAccess class in 2.4.0.
 */
@Deprecated
@JsonDeserialize(builder = FederatedAccess.Builder.class)
@JsonPropertyOrder(value = {"class", "owningUserId"}, alphabetic = true)
public class FederatedAccess implements AccessControlledResource {
    private static final long serialVersionUID = 1399629017857618033L;
    private final boolean isPublic;
    private final Set<String> graphAuths;
    private final String owningUserId;
    private final AccessPredicate readAccessPredicate;
    private final AccessPredicate writeAccessPredicate;

    public FederatedAccess(final Set<String> graphAuths, final String owningUserId) {
        this(graphAuths, owningUserId, Boolean.parseBoolean(DEFAULT_VALUE_IS_PUBLIC));
    }

    public FederatedAccess(final Set<String> graphAuths, final String owningUserId, final boolean isPublic) {
        this(graphAuths, owningUserId, isPublic, null, null);
    }

    public FederatedAccess(
            final Set<String> graphAuths,
            final String owningUserId,
            final boolean isPublic,
            final AccessPredicate readAccessPredicate,
            final AccessPredicate writeAccessPredicate) {

        if (graphAuths != null && readAccessPredicate != null) {
            throw new IllegalArgumentException("Only one of graphAuths or readAccessPredicate should be supplied.");
        }

        this.graphAuths = (graphAuths == null) ? null : unmodifiableSet(graphAuths);
        this.owningUserId = owningUserId;
        this.isPublic = isPublic;

        this.readAccessPredicate = readAccessPredicate;
        this.writeAccessPredicate = writeAccessPredicate;
    }

    public Set<String> getGraphAuths() {
        return (graphAuths != null) ? unmodifiableSet(graphAuths) : null;
    }

    public String getOwningUserId() {
        return owningUserId;
    }

    public boolean isPublic() {
        return isPublic;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final FederatedAccess that = (FederatedAccess) o;

        return new EqualsBuilder()
                .append(isPublic, that.isPublic)
                .append(graphAuths, that.graphAuths)
                .append(owningUserId, that.owningUserId)
                .append(readAccessPredicate, that.readAccessPredicate)
                .append(writeAccessPredicate, that.writeAccessPredicate)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(isPublic)
                .append(graphAuths)
                .append(owningUserId)
                .append(readAccessPredicate)
                .append(writeAccessPredicate)
                .toHashCode();
    }

    @Override
    @JsonIgnore
    public ResourceType getResourceType() {
        return ResourceType.FederatedStoreGraph;
    }

    public boolean hasReadAccess(final User user, final String adminAuth) {
        return getOrDefaultReadAccessPredicate().test(user, adminAuth);
    }

    public boolean hasWriteAccess(final User user, final String adminAuth) {
        return getOrDefaultWriteAccessPredicate().test(user, adminAuth);
    }

    public static AccessPredicate deserialisePredicate(final String predicateJson) {
        try {
            return JSONSerialiser.deserialise(predicateJson, AccessPredicate.class);
        } catch (final SerialisationException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static String serialisePredicate(final AccessPredicate accessPredicate) {
        try {
            return accessPredicate != null ? new String(JSONSerialiser.serialise(accessPredicate), StandardCharsets.UTF_8) : null;
        } catch (final SerialisationException e) {
            throw new IllegalArgumentException("Read and write accessPredicates must be JsonSerialisable", e);
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("isPublic", isPublic)
                .append("graphAuths", graphAuths)
                .append("owningUserId", owningUserId)
                .append("readAccessPredicate", readAccessPredicate)
                .append("writeAccessPredicate", writeAccessPredicate)
                .toString();
    }

    public AccessPredicate getReadAccessPredicate() {
        return isNull(readAccessPredicate) ? null : new AccessPredicate(this.readAccessPredicate.getUserPredicate());
    }

    public AccessPredicate getWriteAccessPredicate() {
        return isNull(writeAccessPredicate) ? null : new AccessPredicate(this.writeAccessPredicate.getUserPredicate());
    }

    @JsonIgnore
    public AccessPredicate getOrDefaultReadAccessPredicate() {
        return this.readAccessPredicate != null ? new AccessPredicate(this.readAccessPredicate.getUserPredicate()) : getDefaultReadAccessPredicate();
    }

    @JsonIgnore
    public AccessPredicate getOrDefaultWriteAccessPredicate() {
        return this.writeAccessPredicate != null ? new AccessPredicate(this.writeAccessPredicate.getUserPredicate()) : getDefaultWriteAccessPredicate();
    }

    private AccessPredicate getDefaultReadAccessPredicate() {
        return new FederatedGraphReadAccessPredicate(owningUserId, graphAuths, isPublic);
    }

    private AccessPredicate getDefaultWriteAccessPredicate() {
        return new FederatedGraphWriteAccessPredicate(owningUserId);
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static class Builder {
        private String owningUserId;
        private Set<String> graphAuths;
        private final Builder self = this;
        private boolean isPublic = false;
        private AccessPredicate readAccessPredicate;
        private AccessPredicate writeAccessPredicate;

        @JsonProperty("graphAuths")
        public Builder graphAuths(final String... opAuth) {
            if (null == opAuth) {
                this.graphAuths = null;
            } else {
                graphAuths(asList(opAuth));
            }
            return self;
        }

        public Builder graphAuths(final Collection<? extends String> graphAuths) {
            if (null == graphAuths) {
                this.graphAuths = null;
            } else {
                final HashSet<String> authSet = new HashSet<>(graphAuths);
                authSet.removeAll(asList("", null));
                this.graphAuths = authSet;
            }
            return self;
        }

        public Builder addGraphAuths(final Collection<? extends String> graphAuths) {
            if (null != graphAuths) {
                final HashSet<String> authSet = new HashSet<>(graphAuths);
                authSet.removeAll(asList("", null));
                if (null == this.graphAuths) {
                    this.graphAuths = authSet;
                } else {
                    this.graphAuths.addAll(authSet);
                }
            }
            return self;
        }

        public Builder owningUserId(final String owningUser) {
            this.owningUserId = owningUser;
            return self;
        }

        public Builder readAccessPredicate(final AccessPredicate readAccessPredicate) {
            this.readAccessPredicate = readAccessPredicate;
            return self;
        }

        public Builder writeAccessPredicate(final AccessPredicate writeAccessPredicate) {
            this.writeAccessPredicate = writeAccessPredicate;
            return self;
        }

        public FederatedAccess build() {
            return new FederatedAccess(graphAuths, owningUserId, isPublic, readAccessPredicate, writeAccessPredicate);
        }

        @JsonIgnore
        public Builder makePublic() {
            isPublic = true;
            return self;
        }

        @JsonIgnore
        public Builder makePrivate() {
            isPublic = false;
            return self;
        }

        @JsonProperty("public")
        public Builder isPublic(final boolean isPublic) {
            this.isPublic = isPublic;
            return self;
        }

        @JsonIgnore
        public Builder clone(final FederatedAccess that) {
            this.graphAuths = that.graphAuths;
            this.owningUserId = that.owningUserId;
            this.isPublic = that.isPublic;
            this.readAccessPredicate = that.readAccessPredicate;
            this.writeAccessPredicate = that.writeAccessPredicate;
            return self;
        }
    }

    static final class Transient implements Serializable {
        private final boolean isPublic;
        private final Set<String> graphAuths;
        private final String owningUserId;
        private final String readAccessPredicate;
        private final String writeAccessPredicate;

        private Transient(final FederatedAccess federatedAccess) {
            this.isPublic = federatedAccess.isPublic;
            this.graphAuths = federatedAccess.graphAuths;
            this.owningUserId = federatedAccess.owningUserId;
            this.readAccessPredicate = serialisePredicate(federatedAccess.readAccessPredicate);
            this.writeAccessPredicate = serialisePredicate(federatedAccess.writeAccessPredicate);
        }

        public static Transient getTransient(final FederatedAccess federatedAccess) {
            return isNull(federatedAccess) ? null : new Transient(federatedAccess);
        }

        public static FederatedAccess getFederatedAccess(final Transient federatedAccessTransient) {
            return isNull(federatedAccessTransient) ? null : new FederatedAccess(federatedAccessTransient.graphAuths, federatedAccessTransient.owningUserId, federatedAccessTransient.isPublic, isNull(federatedAccessTransient.readAccessPredicate) ? null : deserialisePredicate(federatedAccessTransient.readAccessPredicate), isNull(federatedAccessTransient.writeAccessPredicate) ? null : deserialisePredicate(federatedAccessTransient.writeAccessPredicate));
        }
    }
}
