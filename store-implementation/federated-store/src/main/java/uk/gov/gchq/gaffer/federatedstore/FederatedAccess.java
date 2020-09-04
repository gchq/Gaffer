/*
 * Copyright 2017-2020 Crown Copyright
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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.access.AccessControlledResource;
import uk.gov.gchq.gaffer.access.ResourceType;
import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.federatedstore.access.predicate.FederatedGraphReadAccessPredicate;
import uk.gov.gchq.gaffer.federatedstore.access.predicate.FederatedGraphWriteAccessPredicate;
import uk.gov.gchq.gaffer.user.User;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.DEFAULT_VALUE_IS_PUBLIC;

/**
 * Conditions required for a {@link User} to have access to a graph within the
 * {@link FederatedStore} via {@link FederatedAccess}
 * <table summary="FederatedAccess truth table">
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
 *
 * @see #isValidToExecute(User)
 */
public class FederatedAccess implements AccessControlledResource, Serializable {
    private static final long serialVersionUID = 1399629017857618033L;
    private static final boolean NOT_DISABLED_BY_DEFAULT = false;
    private final boolean isPublic;
    private Set<String> graphAuths;
    private String addingUserId;
    private final boolean disabledByDefault;
    private AccessPredicate readAccessPredicate;
    private AccessPredicate writeAccessPredicate;

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
        this.graphAuths = graphAuths;
        this.addingUserId = addingUserId;
        this.isPublic = isPublic;
        this.disabledByDefault = disabledByDefault;

        this.readAccessPredicate = readAccessPredicate != null ? readAccessPredicate : new FederatedGraphReadAccessPredicate(addingUserId, graphAuths, isPublic);
        this.writeAccessPredicate = writeAccessPredicate != null ? writeAccessPredicate : new FederatedGraphWriteAccessPredicate(addingUserId);
    }

    public String getAddingUserId() {
        return addingUserId;
    }

    public boolean isDisabledByDefault() {
        return disabledByDefault;
    }

    /**
     * @param user User request permission.
     * @return boolean permission for user.
     * @Deprecated see {@link FederatedAccess#hasReadAccess(User, String)}
     *
     * <table summary="isValidToExecute truth table">
     * <tr><td> hookAuthsEmpty  </td><td> isAddingUser</td><td>
     * userHasASharedAuth</td><td> isValid?</td></tr>
     * <tr><td>  T              </td><td> T           </td><td> n/a
     * </td><td> T   </td></tr>
     * <tr><td>  T              </td><td> F           </td><td> n/a
     * </td><td> F   </td></tr>
     * <tr><td>  F              </td><td> T           </td><td> n/a
     * </td><td> T   </td></tr>
     * <tr><td>  F              </td><td> n/a         </td><td> T
     * </td><td> T   </td></tr>
     * <tr><td>  F              </td><td> F           </td><td> F
     * </td><td> F   </td></tr>
     * </table>
     */
    protected boolean isValidToExecute(final User user) {
        return isPublic || (null != user && (isAddingUser(user) || (!isAuthsNullOrEmpty() && isUserHasASharedAuth(user))));
    }

    private boolean isUserHasASharedAuth(final User user) {
        return !Collections.disjoint(user.getOpAuths(), this.graphAuths);
    }

    protected boolean isAddingUser(final User user) {
        return null != user.getUserId() && user.getUserId().equals(addingUserId);
    }

    private boolean isAuthsNullOrEmpty() {
        return (null == this.graphAuths || this.graphAuths.isEmpty());
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
    public ResourceType getResourceType() {
        return ResourceType.FederatedStoreGraph;
    }

    public boolean hasReadAccess(final User user, final String adminAuth) {
        return readAccessPredicate.test(user, adminAuth);
    }

    public boolean hasWriteAccess(final User user, final String adminAuth) {
        return writeAccessPredicate.test(user, adminAuth);
    }

    public AccessPredicate getReadAccessPredicate() {
        return readAccessPredicate;
    }

    public AccessPredicate getWriteAccessPredicate() {
        return writeAccessPredicate;
    }

    public static class Builder {
        private String addingUserId;
        private Set<String> graphAuths;
        private final Builder self = this;
        private boolean isPublic = false;
        private boolean disabledByDefault;
        private AccessPredicate readAccessPredicate;
        private AccessPredicate writeAccessPredicate;

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

        public Builder makePublic() {
            isPublic = true;
            return self;
        }

        public Builder makePrivate() {
            isPublic = false;
            return self;
        }

        public Builder clone(final FederatedAccess that) {
            this.graphAuths = that.graphAuths;
            this.addingUserId = that.addingUserId;
            this.isPublic = that.isPublic;
            this.disabledByDefault = that.disabledByDefault;
            this.readAccessPredicate = that.readAccessPredicate;
            this.writeAccessPredicate = that.writeAccessPredicate;
            return self;
        }
    }
}
