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
package uk.gov.gchq.gaffer.data.elementdefinition.view.access.predicate;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.DefaultAccessPredicateTest;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.user.User;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NamedViewWriteAccessPredicateTest extends DefaultAccessPredicateTest {
    private static final User TEST_USER = new User.Builder().userId("TestUser").opAuths("auth1", "auth2").build();

    @Test
    @Override
    public void canBeJsonSerialisedAndDeserialised() throws Exception {
        final AccessPredicate predicate = createAccessPredicate(TEST_USER, asList("auth1", "auth2"));
        final byte[] bytes = JSONSerialiser.serialise(predicate);
        assertEquals("{" +
                "\"class\":\"uk.gov.gchq.gaffer.data.elementdefinition.view.access.predicate.NamedViewWriteAccessPredicate\"," +
                "\"userPredicate\":{" +
                "\"class\":\"uk.gov.gchq.gaffer.data.elementdefinition.view.access.predicate.user.NamedViewWriteUserPredicate\"," +
                "\"creatingUserId\":\"TestUser\"," +
                "\"auths\":[\"auth1\",\"auth2\"]" +
                "}" +
                "}", new String(bytes, CommonConstants.UTF_8));
        assertEquals(predicate, JSONSerialiser.deserialise(bytes, NamedViewWriteAccessPredicate.class));
    }

    @Test
    public void shouldAllowAccessWhenNamedViewCreatorIsNull() {
        assertTrue(new NamedViewWriteAccessPredicate((String) null, asList("a1")).test(TEST_USER, ""));
    }

    @Override
    protected AccessPredicate createAccessPredicate(final User user, final List<String> auths) {
        return new NamedViewWriteAccessPredicate(user, auths);
    }
}
