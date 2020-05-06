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

package uk.gov.gchq.gaffer.user;

import org.hamcrest.core.IsCollectionContaining;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UserTest {

    @Test
    public void shouldBuildUser() {
        // Given
        final String userId = "user 01";
        final String dataAuth1 = "dataAuth 1";
        final String dataAuth2 = "dataAuth 2";
        final String opAuth1 = "opAuth 1";
        final String opAuth2 = "opAuth 2";

        // When
        final User user = new User.Builder()
                .userId(userId)
                .dataAuth(dataAuth1)
                .dataAuth(dataAuth2)
                .opAuth(opAuth1)
                .opAuth(opAuth2)
                .build();

        // Then
        assertEquals(userId, user.getUserId());
        assertEquals(2, user.getDataAuths().size());
        assertThat(user.getDataAuths(), IsCollectionContaining.hasItems(
                dataAuth1, dataAuth2
        ));
        assertEquals(2, user.getOpAuths().size());
        assertThat(user.getOpAuths(), IsCollectionContaining.hasItems(
                opAuth1, opAuth1
        ));
    }

    @ParameterizedTest
    @NullAndEmptySource
    public void shouldReplaceNullIdWithUnknownIdWhenBuildingUser(String userId) {
        final User user = new User.Builder()
                .userId(userId)
                .build();

        assertEquals(User.UNKNOWN_USER_ID, user.getUserId());
    }

    @Test
    public void shouldSetUnknownIdWhenBuildingUser() {
        final User user = new User.Builder()
                .build();

        assertEquals(User.UNKNOWN_USER_ID, user.getUserId());
    }

    @Test
    public void shouldNotAllowChangingDataAuths() {
        // Given
        final String userId = "user 01";
        final String dataAuth1 = "dataAuth 1";
        final String dataAuth2 = "dataAuth 2";
        final String newDataAuth = "new dataAuth";
        final User user = new User.Builder()
                .userId(userId)
                .dataAuth(dataAuth1)
                .dataAuth(dataAuth2)
                .build();

        // When
        assertThrows(UnsupportedOperationException.class, () -> user.getDataAuths().add(newDataAuth));

        // Then
        assertFalse(user.getDataAuths().contains(newDataAuth));
    }

    @Test
    public void shouldNotAllowChangingOpAuths() {
        // Given
        final String userId = "user 01";
        final String opAuth1 = "opAuth 1";
        final String opAuth2 = "opAuth 2";
        final String newOpAuth = "new opAuth";
        final User user = new User.Builder()
                .userId(userId)
                .opAuth(opAuth1)
                .opAuth(opAuth2)
                .build();

        // When
        assertThrows(UnsupportedOperationException.class, () -> user.getOpAuths().add(newOpAuth));

        // Then
        assertFalse(user.getOpAuths().contains(newOpAuth));
    }

    @Test
    public void shouldBeEqualWhen2UsersHaveSameFields() {
        // Given
        final String userId = "user 01";
        final String dataAuth1 = "dataAuth 1";
        final String dataAuth2 = "dataAuth 2";
        final String opAuth1 = "opAuth 1";
        final String opAuth2 = "opAuth 2";

        final User user1 = new User.Builder()
                .userId(userId)
                .dataAuth(dataAuth1)
                .dataAuth(dataAuth2)
                .opAuth(opAuth1)
                .opAuth(opAuth2)
                .build();

        final User user1Clone = new User.Builder()
                .userId(userId)
                .dataAuth(dataAuth1)
                .dataAuth(dataAuth2)
                .opAuth(opAuth1)
                .opAuth(opAuth2)
                .build();

        // Then
        assertEquals(user1, user1Clone);
        assertEquals(user1.hashCode(), user1Clone.hashCode());
    }

    @Test
    public void shouldNotBeEqualWhen2UsersHaveDifferentUserIds() {
        // Given
        final String userId1 = "user 01";
        final String userId2 = "user 02";
        final String dataAuth1 = "dataAuth 1";
        final String dataAuth2 = "dataAuth 2";
        final String opAuth1 = "opAuth 1";
        final String opAuth2 = "opAuth 2";

        final User user1 = new User.Builder()
                .userId(userId1)
                .dataAuth(dataAuth1)
                .dataAuth(dataAuth2)
                .opAuth(opAuth1)
                .opAuth(opAuth2)
                .build();

        final User user2 = new User.Builder()
                .userId(userId2)
                .dataAuth(dataAuth1)
                .dataAuth(dataAuth2)
                .opAuth(opAuth1)
                .opAuth(opAuth2)
                .build();

        // Then
        assertNotEquals(user1, user2);
        assertNotEquals(user1.hashCode(), user2.hashCode());
    }

    @Test
    public void shouldNotBeEqualWhen2UsersHaveDifferentDataAuths() {
        // Given
        final String userId = "user 01";
        final String dataAuth1 = "dataAuth 1";
        final String dataAuth2a = "dataAuth 2a";
        final String dataAuth2b = "dataAuth 2b";
        final User user1 = new User.Builder()
                .userId(userId)
                .dataAuth(dataAuth1)
                .dataAuth(dataAuth2a)
                .build();

        final User user2 = new User.Builder()
                .userId(userId)
                .dataAuth(dataAuth1)
                .dataAuth(dataAuth2b)
                .build();

        // Then
        assertNotEquals(user1, user2);
        assertNotEquals(user1.hashCode(), user2.hashCode());
    }

    @Test
    public void shouldNotBeEqualWhen2UsersHaveDifferentOpAuths() {
        // Given
        final String userId = "user 01";
        final String opAuth1 = "opAuth 1";
        final String opAuth2a = "opAuth 2a";
        final String opAuth2b = "opAuth 2b";
        final User user1 = new User.Builder()
                .userId(userId)
                .opAuth(opAuth1)
                .opAuth(opAuth2a)
                .build();

        final User user2 = new User.Builder()
                .userId(userId)
                .opAuth(opAuth1)
                .opAuth(opAuth2b)
                .build();

        // Then
        assertNotEquals(user1, user2);
        assertNotEquals(user1.hashCode(), user2.hashCode());
    }
}
