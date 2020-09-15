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
import com.fasterxml.jackson.annotation.JsonProperty;

import uk.gov.gchq.gaffer.access.predicate.user.DefaultUserPredicate;
import uk.gov.gchq.gaffer.user.User;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.isNull;

public class FederatedGraphReadUserPredicate extends DefaultUserPredicate {

    private final boolean isPublic;

    @JsonCreator
    public FederatedGraphReadUserPredicate(
            @JsonProperty("creatingUserId") final String creatingUserId,
            @JsonProperty("auths") final List<String> auths,
            @JsonProperty("public") final boolean isPublic) {
        super(creatingUserId, auths);
        this.isPublic = isPublic;
    }

    @Override
    public boolean test(final User user) {
        return isPublic || super.test(user);
    }

    public boolean isPublic() {
        return isPublic;
    }

    @Override
    public boolean hasPermission(final User user) {
        return (!isNull(user)
                && !user.getOpAuths().isEmpty()
                && !this.getAuths().isEmpty()
                && user.getOpAuths().stream().anyMatch(this.getAuths()::contains));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        final FederatedGraphReadUserPredicate that = (FederatedGraphReadUserPredicate) o;
        return isPublic == that.isPublic;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isPublic);
    }
}
