/*
 * Copyright 2017 Crown Copyright
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
public class FederatedAccess implements Serializable {
    private static final long serialVersionUID = 1399629017857618033L;

    private boolean isPublic = Boolean.valueOf(DEFAULT_VALUE_IS_PUBLIC);
    private Set<String> graphAuths = new HashSet<>();
    private String addingUserId;

    public FederatedAccess(final Set<String> graphAuths, final String addingUserId) {
        setGraphAuths(graphAuths);
        setAddingUserId(addingUserId);
    }

    public FederatedAccess(final Set<String> graphAuths, final String addingUser, final boolean isPublic) {
        this(graphAuths, addingUser);
        this.isPublic = isPublic;
    }

    public String getAddingUserId() {
        return addingUserId;
    }

    public void setAddingUserId(final String creatorUserId) {
        this.addingUserId = creatorUserId;
    }

    /**
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
     *
     * @param user User request permission.
     * @return boolean permission for user.
     */
    protected boolean isValidToExecute(final User user) {
        return isPublic || (null != user && (isAddingUser(user) || (!isAuthsNullOrEmpty() && isUserHasASharedAuth(user))));
    }

    private boolean isUserHasASharedAuth(final User user) {
        return !Collections.disjoint(user.getOpAuths(), this.graphAuths);
    }

    private boolean isAddingUser(final User user) {
        return null != user.getUserId() && user.getUserId().equals(addingUserId);
    }

    private boolean isAuthsNullOrEmpty() {
        return (null == this.graphAuths || this.graphAuths.isEmpty());
    }

    public void setGraphAuths(final Set<String> graphAuths) {
        this.graphAuths = graphAuths;
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
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(isPublic)
                .append(graphAuths)
                .append(addingUserId)
                .toHashCode();
    }

    public static class Builder {
        private String addingUser;
        private Set<String> graphAuths;
        private final Builder self = this;
        private boolean isPublic = false;

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
            this.addingUser = addingUser;
            return self;
        }

        public FederatedAccess build() {
            return new FederatedAccess(graphAuths, addingUser, isPublic);
        }

        public Builder makePublic() {
            isPublic = true;
            return self;
        }

        public Builder makePrivate() {
            isPublic = false;
            return self;
        }
    }
}
