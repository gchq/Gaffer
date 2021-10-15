/*
 * Copyright 2020-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.access.predicate.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.predicate.KoryphePredicate;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

@Since("1.13.1")
@Summary("A predicate which returns true if the user is the creatingUser or has a role in the auths list")
public class DefaultUserPredicate extends KoryphePredicate<User> implements Serializable {
    private final String creatingUserId;
    private final List<String> auths;

    @JsonCreator
    public DefaultUserPredicate(
            @JsonProperty("creatingUserId") final String creatingUserId,
            @JsonProperty("auths") final List<String> auths) {
        this.creatingUserId = creatingUserId;
        if (auths != null) {
            this.auths = auths.stream().sorted().collect(Collectors.toList());
        } else {
            this.auths = emptyList();
        }
    }

    @Override
    public boolean equals(final Object o) {
        return (this == o)
                || ((o != null)
                && (this.getClass() == o.getClass())
                && new EqualsBuilder()
                .appendSuper(super.equals(o))
                .append(this.creatingUserId, ((DefaultUserPredicate) o).creatingUserId)
                .append(this.auths, ((DefaultUserPredicate) o).auths)
                .isEquals());

    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(15, 31)
                .appendSuper(super.hashCode())
                .append(getClass())
                .append(creatingUserId)
                .append(auths)
                .hashCode();
    }

    public String getCreatingUserId() {
        return creatingUserId;
    }

    public List<String> getAuths() {
        return auths;
    }

    @Override
    public boolean test(final User user) {
        return isResourceCreator(user) || hasPermission(user);
    }

    public boolean isResourceCreator(final User user) {
        return (!isNull(user)
                && isNotEmpty(user.getUserId())
                && isNotEmpty(this.getCreatingUserId())
                && this.getCreatingUserId().equals(user.getUserId()));
    }

    public boolean hasPermission(final User user) {
        return (!isNull(user)
                && !user.getOpAuths().isEmpty()
                && this.getAuths().stream().anyMatch(user.getOpAuths()::contains));
    }
}
