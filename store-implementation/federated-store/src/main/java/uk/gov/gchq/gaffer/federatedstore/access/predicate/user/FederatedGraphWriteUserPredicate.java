/*
 * Copyright 2020 Crown Copyright
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
package uk.gov.gchq.gaffer.federatedstore.access.predicate.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import uk.gov.gchq.gaffer.access.predicate.user.DefaultUserPredicate;
import uk.gov.gchq.gaffer.user.User;

import java.util.List;

import static java.util.Collections.emptyList;

/**
 * @deprecated Marked for removal in 2.4.0 please use standard
 *             DefaultUserPredicate going forward.
 */
@Deprecated
public class FederatedGraphWriteUserPredicate extends DefaultUserPredicate {

    @JsonCreator
    public FederatedGraphWriteUserPredicate(@JsonProperty("creatingUserId") final String creatingUserId) {
        super(creatingUserId, emptyList());
    }

    @Override
    @JsonIgnore
    public List<String> getAuths() {
        return super.getAuths();
    }

    @Override
    public boolean test(final User user) {
        return super.isResourceCreator(user);
    }
}
