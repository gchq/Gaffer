/*
 * Copyright 2020-2024 Crown Copyright
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

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.user.User;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class DefaultAccessPredicateTest {
    private static final User TEST_USER = new User.Builder().userId("TestUser").opAuths("auth1", "auth2").build();

    @Test
    void shouldReturnTrueForResourceCreator() {
        assertThat(createAccessPredicate(TEST_USER, null).test(TEST_USER, null)).isTrue();
    }

    @Test
    void shouldReturnFalseForUserWhoIsNotResourceCreator() {
        assertThat(createAccessPredicate(TEST_USER, null).test(new User.Builder().userId("AnotherUser").build(), null)).isFalse();
    }

    @Test
    void shouldReturnTrueForAdministrator() {
        assertThat(createAccessPredicate(TEST_USER, null).test(new User.Builder().userId("AdminUser").opAuths("auth1", "auth2").build(), "auth1"))
            .isTrue();
    }

    @Test
    void shouldReturnFalseForUserWhoIsNotAdministrator() {
        assertThat(createAccessPredicate(TEST_USER, null).test(new User.Builder().userId("NonAdminUser").opAuths("auth1", "auth2").build(), "auth3"))
            .isFalse();
    }

    @Test
    void shouldReturnTrueForUserWithPermission() {
        assertThat(createAccessPredicate(TEST_USER, asList("auth1")).test(new User.Builder().userId("AnotherUser").opAuths("auth1", "auth2").build(), null))
            .isTrue();
    }

    @Test
    void shouldReturnFalseForUserWithoutPermission() {
        assertThat(createAccessPredicate(TEST_USER, asList("auth1")).test(new User.Builder().userId("AnotherUser").opAuths("auth3").build(), null))
            .isFalse();
    }

    @Test
    public void canBeJsonSerialisedAndDeserialised() throws Exception {
        final AccessPredicate predicate = createAccessPredicate(TEST_USER, asList("auth1", "auth2"));
        final byte[] bytes = JSONSerialiser.serialise(predicate);
        final String expectedString = "{" +
                "\"class\":\"uk.gov.gchq.gaffer.access.predicate.AccessPredicate\"," +
                "\"userPredicate\":{\"class\":\"uk.gov.gchq.gaffer.access.predicate.user.DefaultUserPredicate\",\"creatingUserId\":\"TestUser\",\"auths\":[\"auth1\",\"auth2\"]}" +
                "}";
        assertThat(new String(bytes, StandardCharsets.UTF_8)).isEqualTo(expectedString);
        assertThat(JSONSerialiser.deserialise(bytes, AccessPredicate.class)).isEqualTo(predicate);
    }

    @Test
    void shouldReturnTrueForEqualObjectComparisonWhenEqual() {
        assertThat(createAccessPredicate(TEST_USER, asList("auth1", "auth2"))).isEqualTo(createAccessPredicate(TEST_USER, asList("auth2", "auth1")));
    }

    @Test
    void shouldReturnFalseForEqualObjectComparisonWhenNotEqual() {
        assertThat(createAccessPredicate(TEST_USER, asList("auth2", "auth1")))
            .isNotEqualTo(createAccessPredicate(TEST_USER, asList("auth1")));
        assertThat(createAccessPredicate(TEST_USER, asList("auth2", "auth1")))
            .isNotEqualTo(createAccessPredicate(new User.Builder().userId("AnotherUser").build(), asList("auth1")));
    }

    protected AccessPredicate createAccessPredicate(final User user, final List<String> auths) {
        return new AccessPredicate(user, auths);
    }
}
