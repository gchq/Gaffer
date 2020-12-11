/*
 * Copyright 2016-2020 Crown Copyright
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
package uk.gov.gchq.gaffer.integration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import uk.gov.gchq.gaffer.user.User;

/**
 * Logic/config for setting up and running store integration tests.
 * All tests will be skipped if the storeProperties variable has not been set
 * prior to running the tests.
 */
public abstract class AbstractStoreIT {

    private User user = new User();

    protected static final int DUPLICATES = 2;

    // Identifiers


    public User getUser() {
        return user;
    }
}
