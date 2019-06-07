/*
 * Copyright 2017-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.user;

public final class StoreUser {

    public static final String ALL_USERS = "allUsers";
    public static final String TEST_USER = "testUser";
    public static final String AUTH_USER = "authUser";
    public static final String AUTH_1 = "auth1";
    public static final String AUTH_2 = "auth2";

    private StoreUser() {
        // private to prevent instantiation
    }

    public static User allUsers() {
        return new User.Builder().opAuth(ALL_USERS).build();
    }

    public static User testUser() {
        return new User.Builder().userId(TEST_USER).opAuth(ALL_USERS).build();
    }

    public static User authUser() {
        return new User.Builder().userId(AUTH_USER).opAuths(ALL_USERS, AUTH_1, AUTH_2).build();
    }

    public static User blankUser() {
        return new User.Builder().build();
    }
}
