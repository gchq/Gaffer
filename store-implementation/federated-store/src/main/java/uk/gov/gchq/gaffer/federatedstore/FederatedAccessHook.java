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
import uk.gov.gchq.gaffer.commonutil.exception.UnauthorisedException;
import uk.gov.gchq.gaffer.graph.hook.GraphHook;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.user.User;
import java.util.Collection;
import java.util.Set;

public class FederatedAccessHook implements GraphHook {
    public static final String USER_DOES_NOT_HAVE_CORRECT_AUTHS_TO_ACCESS_THIS_GRAPH_USER_S = "User does not have correct auths to access this graph. User: %s";
    Set<String> opAuths = Sets.newHashSet();
    Set<String> dataAuths = Sets.newHashSet();

    @Override
    public void preExecute(final OperationChain<?> opChain, final User user) {
        if (!user.getOpAuths().containsAll(opAuths) ||
                !user.getDataAuths().containsAll(dataAuths)) {
            throw new UnauthorisedException(String.format(USER_DOES_NOT_HAVE_CORRECT_AUTHS_TO_ACCESS_THIS_GRAPH_USER_S, user.toString()));
        }

    }

    public boolean addOpAuths(final String opAuth) {
        return this.opAuths.add(opAuth);
    }

    public boolean addOpAuths(final Collection<? extends String> opAuths) {
        return this.opAuths.addAll(opAuths);
    }

    public boolean addDataAuths(final String dataAuth) {
        return this.dataAuths.add(dataAuth);
    }

    public boolean addDataAuths(final Collection<? extends String> dataAuths) {
        return this.dataAuths.addAll(dataAuths);
    }

    @Override
    public <T> T postExecute(final T result, final OperationChain<?> opChain, final User user) {
        return result;
    }

    public static class Builder {
        FederatedAccessHook _hook = new FederatedAccessHook();
        Builder _self = this;

        public Builder addOpAuths(final String opAuth) {
            _hook.addOpAuths(opAuth);
            return _self;
        }

        public Builder addOpAuths(final Collection<? extends String> opAuths) {
            _hook.addOpAuths(opAuths);
            return _self;
        }

        public Builder addDataAuths(final String dataAuth) {
            _hook.dataAuths.add(dataAuth);
            return _self;
        }

        public Builder addDataAuths(final Collection<? extends String> dataAuths) {
            _hook.dataAuths.addAll(dataAuths);
            return _self;
        }

        public FederatedAccessHook build() {
            return _hook;
        }
    }

}
