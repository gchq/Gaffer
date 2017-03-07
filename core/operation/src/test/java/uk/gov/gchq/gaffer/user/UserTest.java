/*
 * Copyright 2016 Crown Copyright
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
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

    @Test
    public void shouldReplaceNullIdWithUnknownIdWhenBuildingUser() {
        // Given
        final String userId = null;

        // When
        final User user = new User.Builder()
                .userId(userId)
                .build();

        // Then
        assertEquals(User.UNKNOWN_USER_ID, user.getUserId());
    }

    @Test
    public void shouldReplaceEmptyIdWithUnknownIdWhenBuildingUser() {
        // Given
        final String userId = "";

        // When
        final User user = new User.Builder()
                .userId(userId)
                .build();

        // Then
        assertEquals(User.UNKNOWN_USER_ID, user.getUserId());
    }

    @Test
    public void shouldSetUnknownIdWhenBuildingUser() {
        // Given
        // When
        final User user = new User.Builder()
                .build();

        // Then
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
        try {
            user.getDataAuths().add(newDataAuth);
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }

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
        try {
            user.getOpAuths().add(newOpAuth);
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }

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

        final User userLocked = new User.Builder()
                .userId(userId)
                .dataAuth(dataAuth1)
                .dataAuth(dataAuth2)
                .opAuth(opAuth1)
                .opAuth(opAuth2)
                .build();

        final User userUnlocked = new User.Builder()
                .userId(userId)
                .dataAuth(dataAuth1)
                .dataAuth(dataAuth2)
                .opAuth(opAuth1)
                .opAuth(opAuth2)
                .build();

        // When
        final boolean isEqual = userLocked.equals(userUnlocked);

        // Then
        assertTrue(isEqual);
        assertEquals(userLocked.hashCode(), userUnlocked.hashCode());
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

        // When
        final boolean isEqual = user1.equals(user2);

        // Then
        assertFalse(isEqual);
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

        // When
        final boolean isEqual = user1.equals(user2);

        // Then
        assertFalse(isEqual);
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

        // When
        final boolean isEqual = user1.equals(user2);

        // Then
        assertFalse(isEqual);
        assertNotEquals(user1.hashCode(), user2.hashCode());
    }
}