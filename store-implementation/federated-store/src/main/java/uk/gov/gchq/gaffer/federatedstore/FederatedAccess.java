/*
 * Copyright 2017-2022 Crown Copyright
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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
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
 */
@JsonDeserialize(builder = FederatedAccess.Builder.class)
@JsonPropertyOrder(value = {"class"}, alphabetic = true)
public class FederatedAccess implements AccessControlledResource, Serializable {
    private static final long serialVersionUID = 1399629017857618033L;
    private static final boolean NOT_DISABLED_BY_DEFAULT = false;
    private final boolean isPublic;
    private final Set<String> graphAuths;
    private final String addingUserId;
    private final boolean disabledByDefault;
    private final String readAccessPredicate;
    private final String writeAccessPredicate;

    public FederatedAccess(final Set<String> graphAuths, final String addingUserId) {
        this(graphAuths, addingUserId, Boolean.valueOf(DEFAULT_VALUE_IS_PUBLIC));
    }

    public FederatedAccess(final Set<String> graphAuths, final String addingUserId, final boolean isPublic) {
        this(graphAuths, addingUserId, isPublic, NOT_DISABLED_BY_DEFAULT);
    }

    public FederatedAccess(final Set<String> graphAuths, final String addingUserId, final boolean isPublic, final boolean disabledByDefault) {
        this(graphAuths, addingUserId, isPublic, disabledByDefault, null, null);
    }

    public FederatedAccess(
            final Set<String> graphAuths,
            final String addingUserId,
            final boolean isPublic,
            final boolean disabledByDefault,
            final AccessPredicate readAccessPredicate,
            final AccessPredicate writeAccessPredicate) {

        if (graphAuths != null && readAccessPredicate != null) {
            throw new IllegalArgumentException("Only one of graphAuths or readAccessPredicate should be supplied.");
        }

        this.graphAuths = graphAuths;
        this.addingUserId = addingUserId;
        this.isPublic = isPublic;
        this.disabledByDefault = disabledByDefault;

        try {
            this.readAccessPredicate = readAccessPredicate != null ? new String(JSONSerialiser.serialise(readAccessPredicate)) : null;
            this.writeAccessPredicate = writeAccessPredicate != null ? new String(JSONSerialiser.serialise(writeAccessPredicate)) : null;
        } catch (final SerialisationException e) {
            throw new IllegalArgumentException("Read and write accessPredicates must be JsonSerialisable", e);
        }
    }

    public Set<String> getGraphAuths() {
        return graphAuths != null ? unmodifiableSet(graphAuths) : null;
    }

    public String getAddingUserId() {
        return addingUserId;
    }

    public boolean isPublic() {
        return isPublic;
    }

    public boolean isDisabledByDefault() {
        return disabledByDefault;
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
                .append(addingUserId, that.addingUserId)
                .append(disabledByDefault, that.disabledByDefault)
                .append(readAccessPredicate, that.readAccessPredicate)
                .append(writeAccessPredicate, that.writeAccessPredicate)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(isPublic)
                .append(graphAuths)
                .append(addingUserId)
                .append(disabledByDefault)
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

    private AccessPredicate deserialisePredicate(final String predicateJson) {
        try {
            return JSONSerialiser.deserialise(predicateJson, AccessPredicate.class);
        } catch (final SerialisationException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("isPublic", isPublic)
                .append("graphAuths", graphAuths)
                .append("addingUserId", addingUserId)
                .append("disabledByDefault", disabledByDefault)
                .append("readAccessPredicate", readAccessPredicate)
                .append("writeAccessPredicate", writeAccessPredicate)
                .toString();
    }

    public AccessPredicate getReadAccessPredicate() {
        return readAccessPredicate != null ? deserialisePredicate(readAccessPredicate) : null;
    }

    public AccessPredicate getWriteAccessPredicate() {
        return writeAccessPredicate != null ? deserialisePredicate(writeAccessPredicate) : null;
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
        return new FederatedGraphReadAccessPredicate(addingUserId, graphAuths, isPublic);
    }

    private AccessPredicate getDefaultWriteAccessPredicate() {
        return new FederatedGraphWriteAccessPredicate(addingUserId);
    }

    @JsonPOJOBuilder(withPrefix = "")
    public static class Builder {
        private String addingUserId;
        private Set<String> graphAuths;
        private final Builder self = this;
        private boolean isPublic = false;
        private boolean disabledByDefault = false;
        private AccessPredicate readAccessPredicate;
        private AccessPredicate writeAccessPredicate;

        @JsonProperty("graphAuths")
        public Builder graphAuths(final String... opAuth) {
            if (null == opAuth) {
                this.graphAuths = null;
            } else {
                graphAuths(Arrays.asList(opAuth));
            }
            return self;
        }

        public Builder graphAuths(final Collection<? extends String> graphAuths) {
            if (null == graphAuths) {
                this.graphAuths = null;
            } else {
                final HashSet<String> authSet = Sets.newHashSet(graphAuths);
                authSet.removeAll(Lists.newArrayList("", null));
                this.graphAuths = authSet;
            }
            return self;
        }

        public Builder addGraphAuths(final Collection<? extends String> graphAuths) {
            if (null != graphAuths) {
                final HashSet<String> authSet = Sets.newHashSet(graphAuths);
                authSet.removeAll(Lists.newArrayList("", null));
                if (null == this.graphAuths) {
                    this.graphAuths = authSet;
                } else {
                    this.graphAuths.addAll(authSet);
                }
            }
            return self;
        }

        public Builder addingUserId(final String addingUser) {
            this.addingUserId = addingUser;
            return self;
        }

        public Builder disabledByDefault(final boolean disabledByDefault) {
            this.disabledByDefault = disabledByDefault;
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
            return new FederatedAccess(graphAuths, addingUserId, isPublic, disabledByDefault, readAccessPredicate, writeAccessPredicate);
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
            this.addingUserId = that.addingUserId;
            this.isPublic = that.isPublic;
            this.disabledByDefault = that.disabledByDefault;
            this.readAccessPredicate = that.getReadAccessPredicate();
            this.writeAccessPredicate = that.getWriteAccessPredicate();
            return self;
        }
    }
}
