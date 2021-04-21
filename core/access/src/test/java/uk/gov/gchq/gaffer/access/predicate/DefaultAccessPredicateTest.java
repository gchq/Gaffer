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

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.user.User;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultAccessPredicateTest implements AccessPredicateTest {
    private static final User TEST_USER = new User.Builder().userId("TestUser").opAuths("auth1", "auth2").build();

    @Test
    public void shouldReturnTrueForResourceCreator() {
        assertTrue(createAccessPredicate(TEST_USER, null).test(TEST_USER, null));
    }

    @Test
    public void shouldReturnFalseForUserWhoIsNotResourceCreator() {
        assertFalse(createAccessPredicate(TEST_USER, null).test(new User.Builder().userId("AnotherUser").build(), null));
    }

    @Test
    public void shouldReturnTrueForAdministrator() {
        assertTrue(createAccessPredicate(TEST_USER, null).test(new User.Builder().userId("AdminUser").opAuths("auth1", "auth2").build(), "auth1"));
    }

    @Test
    public void shouldReturnFalseForUserWhoIsNotAdministrator() {
        assertFalse(createAccessPredicate(TEST_USER, null).test(new User.Builder().userId("NonAdminUser").opAuths("auth1", "auth2").build(), "auth3"));
    }

    @Test
    public void shouldReturnTrueForUserWithPermission() {
        assertTrue(createAccessPredicate(TEST_USER, asList("auth1")).test(new User.Builder().userId("AnotherUser").opAuths("auth1", "auth2").build(), null));
    }

    @Test
    public void shouldReturnFalseForUserWithoutPermission() {
        assertFalse(createAccessPredicate(TEST_USER, asList("auth1")).test(new User.Builder().userId("AnotherUser").opAuths("auth3").build(), null));
    }

    @Test
    @Override
    public void canBeJsonSerialisedAndDeserialised() throws Exception {
        final AccessPredicate predicate = createAccessPredicate(TEST_USER, asList("auth1", "auth2"));
        final byte[] bytes = JSONSerialiser.serialise(predicate);
        assertEquals("{" +
                "\"class\":\"uk.gov.gchq.gaffer.access.predicate.AccessPredicate\"," +
                "\"userPredicate\":{\"class\":\"uk.gov.gchq.gaffer.access.predicate.user.DefaultUserPredicate\",\"creatingUserId\":\"TestUser\",\"auths\":[\"auth1\",\"auth2\"]}" +
                "}", new String(bytes, CommonConstants.UTF_8));
        assertEquals(predicate, JSONSerialiser.deserialise(bytes, AccessPredicate.class));
    }

    @Test
    @Override
    public void shouldReturnTrueForEqualObjectComparisonWhenEqual() {
        assertEquals(
                createAccessPredicate(TEST_USER, asList("auth1", "auth2")),
                createAccessPredicate(TEST_USER, asList("auth2", "auth1")));
    }

    @Test
    @Override
    public void shouldReturnFalseForEqualObjectComparisonWhenNotEqual() {
        assertNotEquals(
                createAccessPredicate(TEST_USER, asList("auth1")),
                createAccessPredicate(TEST_USER, asList("auth2", "auth1")));
        assertNotEquals(
                createAccessPredicate(new User.Builder().userId("AnotherUser").build(), asList("auth1")),
                createAccessPredicate(TEST_USER, asList("auth2", "auth1")));
    }

    protected AccessPredicate createAccessPredicate(final User user, final List<String> auths) {
        return new AccessPredicate(user, auths);
    }
}
