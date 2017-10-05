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

import com.google.common.collect.Sets;

import uk.gov.gchq.gaffer.graph.hook.GraphHook;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.user.User;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Conditions required for a {@link User} to have access to a graph within the {@link FederatedStore} via a {@link FederatedAccessHook}
 * <table summary="FederatedAccessHook truth table">
 * <tr><td> User Ops</td><td> AccessHook Ops</td><td> User added graph  </td><td> hasAccess?</td></tr>
 * <tr><td> 'A'     </td><td> 'A'           </td><td> n/a               </td><td> T         </td></tr>
 * <tr><td> 'A','B' </td><td> 'A'           </td><td> n/a               </td><td> T         </td></tr>
 * <tr><td> 'A'     </td><td> 'A','B'       </td><td> n/a               </td><td> T         </td></tr>
 * <tr><td> 'A'     </td><td> 'B'           </td><td> F                 </td><td> F         </td></tr>
 * <tr><td> 'A'     </td><td> 'B'           </td><td> T                 </td><td> T         </td></tr>
 * <tr><td> n/a     </td><td> {@code null}  </td><td> T                 </td><td> T         </td></tr>
 * <tr><td> n/a     </td><td> {@code null}  </td><td> F                 </td><td> F         </td></tr>
 * <tr><td> n/a     </td><td> {@code empty} </td><td> T                 </td><td> T         </td></tr>
 * <tr><td> n/a     </td><td> {@code empty} </td><td> F                 </td><td> F         </td></tr>
 * </table>
 *
 * @see #isValidToExecute(Context)
 */
public class FederatedAccessHook implements GraphHook {
    public static final String USER_DOES_NOT_HAVE_CORRECT_AUTHS_TO_ACCESS_THIS_GRAPH_USER_S = "User does not have correct auths to access this graph. User: %s";
    private Set<String> graphAuths = new HashSet<>();
    private String addingUserId;

    public void setAddingUserId(final String creatorUserId) {
        this.addingUserId = creatorUserId;
    }

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        if (!isValidToExecute(context)) {
            throw new FederatedAccessException(String.format(USER_DOES_NOT_HAVE_CORRECT_AUTHS_TO_ACCESS_THIS_GRAPH_USER_S, context.getUser().toString()));
        }
    }

    /**
     * <table summary="isValidToExecute truth table">
     * <tr><td> hookAuthsEmpty  </td><td> isAddingUser</td><td> userHasASharedAuth</td><td> isValid?</td></tr>
     * <tr><td>  T              </td><td> T           </td><td> n/a               </td><td> T   </td></tr>
     * <tr><td>  T              </td><td> F           </td><td> n/a               </td><td> F   </td></tr>
     * <tr><td>  F              </td><td> T           </td><td> n/a               </td><td> T   </td></tr>
     * <tr><td>  F              </td><td> n/a         </td><td> T                 </td><td> T   </td></tr>
     * <tr><td>  F              </td><td> F           </td><td> F                 </td><td> F   </td></tr>
     * </table>
     *
     * @param context Context request permission.
     * @return boolean permission for user.
     */
    protected boolean isValidToExecute(final Context context) {
        return isAddingUser(context.getUser()) || (!isAuthsNullOrEmpty() && isUserHasASharedAuth(context));
    }

    private boolean isUserHasASharedAuth(final Context context) {
        return !Collections.disjoint(context.getUser().getOpAuths(), this.graphAuths);
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
    public <T> T postExecute(final T result, final OperationChain<?> opChain, final Context context) {
        return result;
    }

    @Override
    public <T> T onFailure(final T result, final OperationChain<?> opChain, final Context context, final Exception e) {
        return result;
    }

    public static class Builder {
        private final FederatedAccessHook hook = new FederatedAccessHook();
        private final Builder self = this;

        public Builder graphAuths(final String... opAuth) {
            if (null == opAuth) {
                hook.setGraphAuths(null);
            } else {
                graphAuths(Arrays.asList(opAuth));
            }
            return self;
        }

        public Builder graphAuths(final Collection<? extends String> opAuths) {
            if (null == opAuths) {
                hook.setGraphAuths(null);
            } else {
                final HashSet<String> graphAuths = Sets.newHashSet(opAuths);
                graphAuths.remove(null);
                graphAuths.remove("");
                if (null == hook.graphAuths) {
                    hook.graphAuths = Sets.newHashSet();
                }
                hook.graphAuths.addAll(graphAuths);
            }
            return self;
        }

        public FederatedAccessHook build() {
            return hook;
        }
    }
}
