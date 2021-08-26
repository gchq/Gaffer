/*
 * Copyright 2016-2021 Crown Copyright
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

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

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
        assertThat(user.getUserId()).isEqualTo(userId);
        assertThat(user.getDataAuths())
                .hasSize(2)
                .contains(dataAuth1, dataAuth2);
        assertThat(user.getOpAuths())
                .hasSize(2)
                .contains(opAuth1, opAuth1);
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
        assertThat(user.getUserId()).isEqualTo(User.UNKNOWN_USER_ID);
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
        assertThat(user.getUserId()).isEqualTo(User.UNKNOWN_USER_ID);
    }

    @Test
    public void shouldSetUnknownIdWhenBuildingUser() {
        // Given
        // When
        final User user = new User.Builder()
                .build();

        // Then
        assertThat(user.getUserId()).isEqualTo(User.UNKNOWN_USER_ID);
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
        final Set<String> dataAuths = user.getDataAuths();

        // Then
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> dataAuths.add(newDataAuth));
        assertThat(dataAuths).doesNotContain(newDataAuth);
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
        final Set<String> opAuths = user.getOpAuths();

        // Then
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> opAuths.add(newOpAuth));
        assertThat(opAuths).doesNotContain(newOpAuth);
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

        // Then
        assertThat(userLocked)
                .isEqualTo(userUnlocked)
                .hasSameHashCodeAs(userUnlocked);
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
        assertThat(user1)
                .isNotEqualTo(user2)
                .doesNotHaveSameHashCodeAs(user2);
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
        assertThat(user1)
                .isNotEqualTo(user2)
                .doesNotHaveSameHashCodeAs(user2);
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
        assertThat(user1)
                .isNotEqualTo(user2)
                .doesNotHaveSameHashCodeAs(user2);
    }
}
