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

package uk.gov.gchq.gaffer.access.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import uk.gov.gchq.gaffer.user.User;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.sort;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public class DefaultAccessPredicate extends AccessPredicate {
    private final String creatingUserId;
    private final List<String> auths;

    @JsonCreator
    public DefaultAccessPredicate(
            @JsonProperty("creatingUserId") final String creatingUserId,
            @JsonProperty("auths") final List<String> auths) {
        this.creatingUserId = creatingUserId;
        if (auths != null) {
            sort(auths);
            this.auths = auths;
        } else {
            this.auths = emptyList();
        }
    }

    public DefaultAccessPredicate(
            final User creatingUser,
            final List<String> auths) {
        this(creatingUser.getUserId(), auths);
    }


    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("creatingUserId", creatingUserId)
                .append("auths", auths)
                .toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final DefaultAccessPredicate that = (DefaultAccessPredicate) o;

        return new EqualsBuilder()
                .append(creatingUserId, that.creatingUserId)
                .append(auths, that.auths)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(creatingUserId)
                .append(auths)
                .toHashCode();
    }

    public String getCreatingUserId() {
        return creatingUserId;
    }

    public List<String> getAuths() {
        return auths;
    }

    @Override
    public boolean test(final User user, final String adminAuth) {
        return isResourceCreator(user) || isAdministrator(user, adminAuth) || hasPermission(user);
    }

    protected boolean isResourceCreator(final User user) {
        return (!isNull(user)
                && isNotEmpty(user.getUserId())
                && isNotEmpty(this.getCreatingUserId())
                && this.getCreatingUserId().equals(user.getUserId()));
    }

    protected boolean isAdministrator(final User user, final String adminAuth) {
        return (!isNull(user)
                && isNotEmpty(adminAuth)
                && user.getOpAuths().contains(adminAuth));
    }

    protected boolean hasPermission(final User user) {
        return (!isNull(user)
                && !user.getOpAuths().isEmpty()
                && this.getAuths().stream().anyMatch(user.getOpAuths()::contains));
    }
}
