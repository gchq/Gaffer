/*
 * Copyright 2020-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.access.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import uk.gov.gchq.gaffer.federatedstore.access.predicate.user.FederatedGraphReadUserPredicate;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;

/**
 * @deprecated Marked for removal in 2.4.0 please use standard AccessPredicates
 *             going forward.
 */
@Deprecated
public class FederatedGraphReadAccessPredicate extends FederatedGraphAccessPredicate {

    public FederatedGraphReadAccessPredicate(final String creatingUserId, final Set<String> auths, final boolean isPublic) {
        this(creatingUserId, auths != null ? new ArrayList<>(auths) : emptyList(), isPublic);
    }

    @JsonCreator
    public FederatedGraphReadAccessPredicate(
            @JsonProperty("creatingUserId") final String creatingUserId,
            @JsonProperty("auths") final List<String> auths,
            @JsonProperty("public") final boolean isPublic) {
        super(new FederatedGraphReadUserPredicate(creatingUserId, auths, isPublic));
    }


    @Override
    public boolean test(final User user, final String adminAuth) {
        return super.test(user, adminAuth);
    }
}
