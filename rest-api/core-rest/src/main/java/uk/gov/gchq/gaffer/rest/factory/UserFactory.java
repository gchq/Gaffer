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

import uk.gov.gchq.gaffer.rest.SystemProperty;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.user.User;

/**
 * A {@code UserFactory} creates instances of {@link User}s for use when executing
 * queries on a graph.
 */
public interface UserFactory {

    static UserFactory createUserFactory() {
        final String userFactoryClass = System.getProperty(SystemProperty.USER_FACTORY_CLASS,
                SystemProperty.USER_FACTORY_CLASS_DEFAULT);

        try {
            return Class.forName(userFactoryClass)
                    .asSubclass(UserFactory.class)
                    .newInstance();
        } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException("Unable to create user factory from class: " + userFactoryClass, e);
        }
    }

    /**
     * Create a new {@link User} object.
     *
     * @return a new user
     */
    User createUser();

    /**
     * Create a new {@link Context} object.
     *
     * @return a new context containing a User object
     */
    default Context createContext() {
        return new Context(createUser());
    }
}
