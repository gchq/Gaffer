/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.rest.factory;

import uk.gov.gchq.gaffer.user.User;

import static uk.gov.gchq.gaffer.user.User.UNKNOWN_USER_ID;

/**
 * Default implementation of the {@link UserFactory} interface. This default implementation
 * always returns an empty {@link User} object (representing an unknown user).
 */
public class UnknownUserFactory implements UserFactory {

    public UnknownUserFactory() {
        // User factories should be constructed via the createGraphFactory static method,
        // public constructor is required only by HK2
    }

    @Override
    public User createUser() {
        return new User.Builder()
                .userId(UNKNOWN_USER_ID)
                .opAuth("user")
                .build();
    }
}
