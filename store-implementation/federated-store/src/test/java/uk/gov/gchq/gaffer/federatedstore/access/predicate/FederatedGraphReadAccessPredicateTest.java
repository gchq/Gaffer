/*
 * Copyright 2020-2022 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.access.predicate;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.AccessPredicateTest;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.user.User;

import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.user.StoreUser.AUTH_USER_ID;
import static uk.gov.gchq.gaffer.user.StoreUser.authUser;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedGraphReadAccessPredicateTest implements AccessPredicateTest {
    private static final List<String> NO_AUTHS = null;
    private static final boolean PUBLIC = true;
    private static final boolean NON_PUBLIC = false;

    @Test
    public void shouldReturnTrueForResourceCreator() {
        assertTrue(createAccessPredicate(AUTH_USER_ID, NO_AUTHS, NON_PUBLIC).test(authUser(), null));
    }

    @Test
    public void shouldReturnTrueForPublicResource() {
        assertTrue(createAccessPredicate(AUTH_USER_ID, asList("auth1"), PUBLIC).test(testUser(), null));
    }

    @Test
    public void shouldReturnFalseForUserWhoIsNotResourceCreator() {
        assertFalse(createAccessPredicate(AUTH_USER_ID, NO_AUTHS, NON_PUBLIC).test(testUser(), null));
    }

    @Test
    public void shouldReturnTrueForAdministrator() {
        assertTrue(createAccessPredicate(AUTH_USER_ID, NO_AUTHS, NON_PUBLIC).test(new User.Builder().userId("AdminUser").opAuths("auth1", "auth2").build(), "auth1"));
    }

    @Test
    public void shouldReturnTrueForAdministratorWithAnyAdminAuths() {
        assertTrue(createAccessPredicate(AUTH_USER_ID, NO_AUTHS, NON_PUBLIC).test(new User.Builder().userId("AdminUser").opAuths("auth1", "auth2").build(), "authX,authY,auth1"));
    }

    @Test
    public void shouldReturnFalseForUserWhoIsNotAdministrator() {
        assertFalse(createAccessPredicate(AUTH_USER_ID, NO_AUTHS, NON_PUBLIC).test(new User.Builder().userId("NonAdminUser").opAuths("auth1", "auth2").build(), "auth3"));
    }

    @Test
    public void shouldReturnTrueForUserWithPermission() {
        assertTrue(createAccessPredicate(AUTH_USER_ID, asList("auth1"), NON_PUBLIC).test(new User.Builder().userId("AnotherUser").opAuths("auth1", "auth2").build(), null));
    }

    @Test
    public void shouldReturnFalseForUserWithoutPermission() {
        assertFalse(createAccessPredicate(AUTH_USER_ID, asList("auth1"), NON_PUBLIC).test(new User.Builder().userId("AnotherUser").opAuths("auth3").build(), null));
    }

    @Test
    public void canBeJsonSerialisedAndDeserialised() throws Exception {
        final AccessPredicate predicate = createAccessPredicate(AUTH_USER_ID, asList("auth1", "auth2"), NON_PUBLIC);
        final byte[] bytes = JSONSerialiser.serialise(predicate);
        assertEquals("{" +
                "\"class\":\"uk.gov.gchq.gaffer.federatedstore.access.predicate.FederatedGraphReadAccessPredicate\"," +
                "\"userPredicate\":{\"class\":\"uk.gov.gchq.gaffer.federatedstore.access.predicate.user.FederatedGraphReadUserPredicate\",\"creatingUserId\":\"authUser\",\"auths\":[\"auth1\",\"auth2\"],\"public\":false}" +
                "}", new String(bytes, CommonConstants.UTF_8));
        assertEquals(predicate, JSONSerialiser.deserialise(bytes, FederatedGraphReadAccessPredicate.class));
    }

    @Test
    public void shouldReturnTrueForEqualObjectComparisonWhenEqual() {
        assertEquals(
                createAccessPredicate(AUTH_USER_ID, asList("auth1", "auth2"), PUBLIC),
                createAccessPredicate(AUTH_USER_ID, asList("auth2", "auth1"), PUBLIC));
    }

    @Test
    public void shouldReturnFalseForEqualObjectComparisonWhenNotEqual() {
        assertThat(createAccessPredicate(AUTH_USER_ID, asList("auth2", "auth1"), PUBLIC)).isNotEqualTo(createAccessPredicate(AUTH_USER_ID, asList("auth1", "auth2"), NON_PUBLIC))
                .isNotEqualTo(createAccessPredicate("anotherUserId", asList("auth1", "auth2"), PUBLIC))
                .isNotEqualTo(createAccessPredicate(AUTH_USER_ID, NO_AUTHS, PUBLIC));
    }

    private AccessPredicate createAccessPredicate(final String creatingUserId, final List<String> auths, final boolean isPublic) {
        return new FederatedGraphReadAccessPredicate(creatingUserId, auths, isPublic);
    }
}
