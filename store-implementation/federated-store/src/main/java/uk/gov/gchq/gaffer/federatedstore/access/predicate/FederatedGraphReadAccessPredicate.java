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

package uk.gov.gchq.gaffer.federatedstore.access.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Objects.isNull;

public class FederatedGraphReadAccessPredicate extends FederatedGraphAccessPredicate {
    private final boolean isPublic;

    public FederatedGraphReadAccessPredicate(final String creatingUserId, final Set<String> auths, final boolean isPublic) {
        this(creatingUserId, auths != null ? new ArrayList<>(auths) : emptyList(), isPublic);
    }

    @JsonCreator
    public FederatedGraphReadAccessPredicate(
            @JsonProperty("creatingUserId") final String creatingUserId,
            @JsonProperty("auths") final List<String> auths,
            @JsonProperty("public") final boolean isPublic) {
        super(creatingUserId, auths);
        this.isPublic = isPublic;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final FederatedGraphReadAccessPredicate that = (FederatedGraphReadAccessPredicate) o;

        return new EqualsBuilder()
                .appendSuper(super.equals(o))
                .append(isPublic, that.isPublic)
                .isEquals();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("isPublic", isPublic)
                .toString();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(isPublic)
                .toHashCode();
    }

    public boolean isPublic() {
        return isPublic;
    }

    @Override
    public boolean test(final User user, final String adminAuth) {
        return isPublic || super.test(user, adminAuth);
    }

    @Override
    protected boolean hasPermission(final User user) {
        return (!isNull(user)
                && !user.getOpAuths().isEmpty()
                && !this.getAuths().isEmpty()
                && user.getOpAuths().stream().anyMatch(this.getAuths()::contains));
    }
}
