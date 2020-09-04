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
package uk.gov.gchq.gaffer.access.predicate.user;

import uk.gov.gchq.gaffer.user.User;

import java.util.Objects;
import java.util.function.Predicate;

public class UnrestrictedAccessUserPredicate implements Predicate<User> {
    private final boolean unrestrictedAccess = true;

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final UnrestrictedAccessUserPredicate that = (UnrestrictedAccessUserPredicate) o;
        return unrestrictedAccess == that.unrestrictedAccess;
    }

    @Override
    public int hashCode() {
        return Objects.hash(unrestrictedAccess);
    }

    @Override
    public boolean test(final User user) {
        return unrestrictedAccess;
    }
}
