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

import com.fasterxml.jackson.annotation.JsonIgnore;

import uk.gov.gchq.gaffer.access.predicate.user.NoAccessUserPredicate;
import uk.gov.gchq.gaffer.user.User;

import java.util.function.Predicate;

/**
 * An {@link AccessPredicate} which never allows user access even if they are an administrator
 */
public class NoAccessPredicate extends AccessPredicate {
    private static final boolean NOT_ADMINISTRATOR = false;

    public NoAccessPredicate() {
        super(new NoAccessUserPredicate());
    }

    @Override
    @JsonIgnore
    public Predicate<User> getUserPredicate() {
        return super.getUserPredicate();
    }

    @Override
    protected boolean isAdministrator(final User user, final String adminAuth) {
        return NOT_ADMINISTRATOR;
    }
}
