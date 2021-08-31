/*
 * Copyright 2020-2021 Crown Copyright
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FederatedGraphWriteAccessPredicateTest implements AccessPredicateTest {
    private static final User TEST_USER = new User.Builder().userId("TestUser").opAuths("auth1", "auth2").build();

    @Test
    public void shouldReturnTrueForResourceCreator() {
        assertTrue(createAccessPredicate(TEST_USER.getUserId()).test(TEST_USER, null));
    }

    @Test
    public void shouldReturnFalseForUserWhoIsNotResourceCreator() {
        assertFalse(createAccessPredicate(TEST_USER.getUserId()).test(new User.Builder().userId("AnotherUser").build(), null));
    }

    @Test
    public void shouldReturnTrueForAdministrator() {
        assertTrue(createAccessPredicate(TEST_USER.getUserId()).test(new User.Builder().userId("AdminUser").opAuths("auth1", "auth2").build(), "auth1"));
    }

    @Test
    public void shouldReturnTrueForAdministratorWithAnyAdminAuths() {
        assertTrue(createAccessPredicate(TEST_USER.getUserId()).test(new User.Builder().userId("AdminUser").opAuths("auth1", "auth2").build(), "authX,authY,auth1"));
    }

    @Test
    public void shouldReturnFalseForUserWhoIsNotAdministrator() {
        assertFalse(createAccessPredicate(TEST_USER.getUserId()).test(new User.Builder().userId("NonAdminUser").opAuths("auth1", "auth2").build(), "auth3"));
    }

    @Test
    public void canBeJsonSerialisedAndDeserialised() throws Exception {
        final AccessPredicate predicate = createAccessPredicate(TEST_USER.getUserId());
        final byte[] bytes = JSONSerialiser.serialise(predicate);
        assertEquals("{" +
                "\"class\":\"uk.gov.gchq.gaffer.federatedstore.access.predicate.FederatedGraphWriteAccessPredicate\"," +
                "\"userPredicate\":{\"class\":\"uk.gov.gchq.gaffer.federatedstore.access.predicate.user.FederatedGraphWriteUserPredicate\",\"creatingUserId\":\"TestUser\"}" +
                "}", new String(bytes, CommonConstants.UTF_8));
        assertEquals(predicate, JSONSerialiser.deserialise(bytes, FederatedGraphWriteAccessPredicate.class));
    }

    @Test
    @Override
    public void shouldReturnTrueForEqualObjectComparisonWhenEqual() {
        assertEquals(
                createAccessPredicate(TEST_USER.getUserId()),
                createAccessPredicate(TEST_USER.getUserId()));
    }

    @Test
    public void shouldReturnFalseForEqualObjectComparisonWhenNotEqual() {
        assertThat(createAccessPredicate(TEST_USER.getUserId())).isNotEqualTo(createAccessPredicate("anotherUserId"));
    }

    private AccessPredicate createAccessPredicate(final String creatingUserId) {
        return new FederatedGraphWriteAccessPredicate(creatingUserId);
    }
}
