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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import com.google.common.collect.Sets;
import uk.gov.gchq.gaffer.graph.hook.GraphHook;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.user.User;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * <table summary="FederatedAccessHook truth table">
 * <tr><td> User Ops</td><td>|</td><td> AccessHook Ops</td><td>|</td><td> User added graph</td><td>|</td><td> hasAccess?</td></tr>
 * <tr><td>  'A'    </td><td>|</td><td> 'A'     </td><td>|</td><td> Y/N </td><td>|</td><td> Y   </td></tr>
 * <tr><td>  'A', 'B' </td><td>|</td><td> 'A'     </td><td>|</td><td> Y/N </td><td>|</td><td> Y   </td></tr>
 * <tr><td>  'A'    </td><td>|</td><td> 'A', 'B'  </td><td>|</td><td> Y/N </td><td>|</td><td> Y   </td></tr>
 * <tr><td>  'A'    </td><td>|</td><td> 'B'     </td><td>|</td><td> N   </td><td>|</td><td> N   </td></tr>
 * <tr><td>  'A'    </td><td>|</td><td> 'B'     </td><td>|</td><td> Y   </td><td>|</td><td> Y   </td></tr>
 * </table>
 */
public class FederatedAccessHook implements GraphHook {
    public static final String USER_DOES_NOT_HAVE_CORRECT_AUTHS_TO_ACCESS_THIS_GRAPH_USER_S = "User does not have correct auths to access this graph. User: %s";
    private Set<String> graphAuths = Sets.newHashSet();
    private String creatorUserId;

    void setCreatorUserId(final String creatorUserId) {
        this.creatorUserId = creatorUserId;
    }

    @Override
    public void preExecute(final OperationChain<?> opChain, final User user) {
        if (!isValidToExecute(user)) {
            throw new FederatedAccessException(String.format(USER_DOES_NOT_HAVE_CORRECT_AUTHS_TO_ACCESS_THIS_GRAPH_USER_S, user.toString()));
        }
    }

    public boolean isValidToExecute(final User user) {
        return (null != user.getUserId() && user.getUserId().equals(creatorUserId)) || !Collections.disjoint(user.getOpAuths(), this.graphAuths);
    }

    public boolean addGraphAuths(final String opAuth) {
        return this.graphAuths.add(opAuth);
    }

    public boolean addGraphAuths(final Collection<? extends String> opAuths) {
        return this.graphAuths.addAll(opAuths);
    }

    @Override
    public <T> T postExecute(final T result, final OperationChain<?> opChain, final User user) {
        return result;
    }

    public static class Builder {
        private FederatedAccessHook hook = new FederatedAccessHook();
        private Builder self = this;

        public Builder graphAuths(final String... opAuth) {
            hook.addGraphAuths(Arrays.asList(opAuth));
            return self;
        }

        public Builder graphAuths(final Collection<? extends String> opAuths) {
            hook.addGraphAuths(opAuths);
            return self;
        }

        public FederatedAccessHook build() {
            return hook;
        }
    }

    public class FederatedAccessException extends RuntimeException {
        public FederatedAccessException() {
        }

        public FederatedAccessException(final String message) {
            super(message);
        }

        public FederatedAccessException(final String message, final Throwable cause) {
            super(message, cause);
        }

        public FederatedAccessException(final Throwable cause) {
            super(cause);
        }

        public FederatedAccessException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
    }
}
