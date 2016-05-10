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

package gaffer.user;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Sets;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;

public class UserTest {
    @Test
    public void shouldBuildUser() {
        // Given
        final String userId = "user 01";
        final String auth1 = "auth 1";
        final String auth2 = "auth 2";

        // When
        final User user = new User.Builder()
                .userId(userId)
                .dataAuth(auth1)
                .dataAuth(auth2)
                .lock()
                .build();

        // Then
        assertEquals(userId, user.getUserId());
        assertTrue(user.isLocked());
        assertEquals(2, user.getDataAuths().size());
        assertThat(user.getDataAuths(), IsCollectionContaining.hasItems(
                auth1, auth2
        ));
    }

    @Test
    public void shouldNotAllowChangingAuths() {
        // Given
        final String userId = "user 01";
        final String auth1 = "auth 1";
        final String auth2 = "auth 2";
        final String newAuth = "new auth";
        final User user = new User.Builder()
                .userId(userId)
                .dataAuth(auth1)
                .dataAuth(auth2)
                .build();

        // When
        try {
            user.getDataAuths().add(newAuth);
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }

        // Then
        assertFalse(user.getDataAuths().contains(newAuth));
    }

    @Test
    public void shouldNotAllowNewAuthWhenUserIsLocked() {
        // Given
        final String userId = "user 01";
        final String auth1 = "auth 1";
        final String auth2 = "auth 2";
        final String newAuth = "new auth";
        final User user = new User.Builder()
                .userId(userId)
                .dataAuth(auth1)
                .dataAuth(auth2)
                .lock()
                .build();

        // When
        try {
            user.addDataAuth(newAuth);
            fail("Exception expected");
        } catch (final IllegalAccessError e) {
            assertNotNull(e.getMessage());
        }

        // Then
        assertFalse(user.getDataAuths().contains(newAuth));
    }

    @Test
    public void shouldNotAllowNewAuthsWhenUserIsLocked() {
        // Given
        final String userId = "user 01";
        final String auth1 = "auth 1";
        final String auth2 = "auth 2";
        final String newAuth = "new auth";
        final User user = new User.Builder()
                .userId(userId)
                .dataAuth(auth1)
                .dataAuth(auth2)
                .lock()
                .build();

        // When
        try {
            user.setDataAuths(Sets.newHashSet(newAuth));
            fail("Exception expected");
        } catch (final IllegalAccessError e) {
            assertNotNull(e.getMessage());
        }

        // Then
        assertFalse(user.getDataAuths().contains(newAuth));
    }

    @Test
    public void shouldNotAllowNewUserIdUserIsLocked() {
        // Given
        final String userId = "user 01";
        final String auth1 = "auth 1";
        final String auth2 = "auth 2";
        final String newUserId = "new user id";
        final User user = new User.Builder()
                .userId(userId)
                .dataAuth(auth1)
                .dataAuth(auth2)
                .lock()
                .build();

        // When
        try {
            user.setUserId(newUserId);
            fail("Exception expected");
        } catch (final IllegalAccessError e) {
            assertNotNull(e.getMessage());
        }

        // Then
        assertEquals(userId, user.getUserId());
    }

    @Test
    public void shouldBeEqualWhen2UsersHaveSameFieldsButOneIsLocked() {
        // Given
        final String userId = "user 01";
        final String auth1 = "auth 1";
        final String auth2 = "auth 2";
        final User userLocked = new User.Builder()
                .userId(userId)
                .dataAuth(auth1)
                .dataAuth(auth2)
                .lock()
                .build();

        final User userUnlocked = new User.Builder()
                .userId(userId)
                .dataAuth(auth1)
                .dataAuth(auth2)
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
        final String auth1 = "auth 1";
        final String auth2 = "auth 2";
        final User user1 = new User.Builder()
                .userId(userId1)
                .dataAuth(auth1)
                .dataAuth(auth2)
                .build();

        final User user2 = new User.Builder()
                .userId(userId2)
                .dataAuth(auth1)
                .dataAuth(auth2)
                .build();

        // When
        final boolean isEqual = user1.equals(user2);

        // Then
        assertFalse(isEqual);
        assertNotEquals(user1.hashCode(), user2.hashCode());
    }

    @Test
    public void shouldNotBeEqualWhen2UsersHaveDifferentAuths() {
        // Given
        final String userId = "user 01";
        final String auth1 = "auth 1";
        final String auth2a = "auth 2a";
        final String auth2b = "auth 2b";
        final User user1 = new User.Builder()
                .userId(userId)
                .dataAuth(auth1)
                .dataAuth(auth2a)
                .build();

        final User user2 = new User.Builder()
                .userId(userId)
                .dataAuth(auth1)
                .dataAuth(auth2b)
                .build();

        // When
        final boolean isEqual = user1.equals(user2);

        // Then
        assertFalse(isEqual);
        assertNotEquals(user1.hashCode(), user2.hashCode());
    }
}