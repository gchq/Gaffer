/*
 * Copyright 2020-2023 Crown Copyright
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
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import uk.gov.gchq.gaffer.access.predicate.user.DefaultUserPredicate;
import uk.gov.gchq.gaffer.user.User;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * A {@link BiPredicate} which will first check if the user is an admin according to the provided
 * admin role. If not it uses a predicate to determine if the user can access a resource.
 */
@SuppressFBWarnings(value = "SE_BAD_FIELD", justification = "Gets serialised by the JSC cache")
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "class")
public class AccessPredicate implements BiPredicate<User, String>, Serializable {

    private final Predicate<User> userPredicate;

    public AccessPredicate(
            final User creatingUser,
            final List<String> auths) {
        this(creatingUser.getUserId(), auths);
    }

    public AccessPredicate(
            final String creatingUserId,
            final List<String> auths) {
        this(new DefaultUserPredicate(creatingUserId, auths));
    }

    @JsonCreator
    public AccessPredicate(@JsonProperty("userPredicate") final Predicate<User> userPredicate) {
        this.userPredicate = userPredicate;
    }

    @Override
    public boolean test(final User user, final String adminAuth) {
        return isAdministrator(user, adminAuth) || this.userPredicate.test(user);
    }

    protected boolean isAdministrator(final User user, final String adminAuth) {
        return (!isNull(user)
                && isNotEmpty(adminAuth)
                && user.getOpAuths().contains(adminAuth));
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
    public Predicate<User> getUserPredicate() {
        return userPredicate;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AccessPredicate predicate = (AccessPredicate) o;
        return Objects.equals(userPredicate, predicate.userPredicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userPredicate);
    }
}
