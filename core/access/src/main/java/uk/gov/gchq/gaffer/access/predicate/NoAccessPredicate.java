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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import uk.gov.gchq.gaffer.user.User;

import java.util.List;

import static java.util.Collections.emptyList;

public class NoAccessPredicate extends AccessPredicate {
    private final boolean noAccess = false;
    private final List<String> auths = emptyList();

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("noAccess", noAccess)
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

        final NoAccessPredicate that = (NoAccessPredicate) o;

        return new EqualsBuilder()
                .append(noAccess, that.noAccess)
                .append(auths, that.auths)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(noAccess)
                .append(auths)
                .toHashCode();
    }

    @Override
    public List<String> getAuths() {
        return auths;
    }

    @Override
    public boolean test(final User user, final String s) {
        return noAccess;
    }
}
