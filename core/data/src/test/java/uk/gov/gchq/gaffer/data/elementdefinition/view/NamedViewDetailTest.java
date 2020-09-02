/*
 * Copyright 2017-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.data.elementdefinition.view;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.UnrestrictedAccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.user.CustomUserPredicate;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.data.elementdefinition.view.access.predicate.NamedViewWriteAccessPredicate;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.user.User;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NamedViewDetailTest {
    private static final AccessPredicate READ_ACCESS_PREDICATE = new AccessPredicate(new CustomUserPredicate(), asList("CustomReadAuth1", "CustomReadAuth2"));
    private static final AccessPredicate WRITE_ACCESS_PREDICATE = new AccessPredicate(new CustomUserPredicate(), asList("CustomWriteAuth1", "CustomWriteAuth2"));

    @Test
    public void shouldJsonSerialise() throws SerialisationException {
        // Given
        final NamedViewDetail namedViewDetail = createNamedViewDetailBuilder()
                .readAccessPredicate(READ_ACCESS_PREDICATE)
                .writeAccessPredicate(WRITE_ACCESS_PREDICATE)
                .build();

        // When
        final byte[] json = JSONSerialiser.serialise(namedViewDetail, true);

        // Then
        final String expected = String.format("{%n" +
                "  \"name\" : \"view1\",%n" +
                "  \"description\" : \"description\",%n" +
                "  \"creatorId\" : \"creator\",%n" +
                "  \"writeAccessRoles\" : [ \"writeAuth1\", \"writeAuth2\" ],%n" +
                "  \"parameters\" : {%n" +
                "    \"entityGroup\" : {%n" +
                "      \"description\" : \"some description\",%n" +
                "      \"defaultValue\" : \"red\",%n" +
                "      \"valueClass\" : \"java.lang.String\",%n" +
                "      \"required\" : false%n" +
                "    }%n" +
                "  },%n" +
                "  \"view\" : \"{\\\"entities\\\": {\\\"${entityGroup}\\\":{}}}\",%n" +
                "  \"auths\" : [ \"CustomReadAuth1\", \"CustomReadAuth2\", \"CustomWriteAuth1\", \"CustomWriteAuth2\" ],%n" +
                "  \"readAccessPredicate\" : {%n" +
                "       \"class\" : \"uk.gov.gchq.gaffer.access.predicate.AccessPredicate\",%n" +
                "       \"userPredicate\" : {%n" +
                "           \"class\" : \"uk.gov.gchq.gaffer.access.predicate.user.CustomUserPredicate\"%n" +
                "       },%n" +
                "       \"auths\" : [ \"CustomReadAuth1\", \"CustomReadAuth2\" ]%n" +
                "   },%n" +
                "   \"writeAccessPredicate\" : {%n" +
                "       \"class\" : \"uk.gov.gchq.gaffer.access.predicate.AccessPredicate\",%n" +
                "       \"userPredicate\" : {%n" +
                "           \"class\" : \"uk.gov.gchq.gaffer.access.predicate.user.CustomUserPredicate\"%n" +
                "       },%n " +
                "       \"auths\" : [ \"CustomWriteAuth1\", \"CustomWriteAuth2\" ]%n" +
                "   }%n" +
                "},%n");
        JsonAssert.assertEquals(expected, new String(json));
    }

    @Test
    public void shouldTestAccessUsingCustomAccessPredicatesWhenConfigured() {
        final User testUser = new User.Builder().userId("testUserId").build();
        final String adminAuth = "adminAuth";
        final AccessPredicate readAccessPredicate = mock(AccessPredicate.class);
        final AccessPredicate writeAccessPredicate = mock(AccessPredicate.class);

        when(readAccessPredicate.test(testUser, adminAuth)).thenReturn(true);
        when(readAccessPredicate.getAuths()).thenReturn(asList("c1", "b1", "a1"));

        when(writeAccessPredicate.test(testUser, adminAuth)).thenReturn(false);
        when(writeAccessPredicate.getAuths()).thenReturn(asList("a1", "z1", "c1", "x1"));

        final NamedViewDetail namedViewDetail = createNamedViewDetailBuilder()
                .readAccessPredicate(readAccessPredicate)
                .writeAccessPredicate(writeAccessPredicate)
                .build();

        assertTrue(namedViewDetail.hasReadAccess(testUser, adminAuth));
        assertFalse(namedViewDetail.hasWriteAccess(testUser, adminAuth));
        assertEquals(asList("a1", "b1", "c1", "x1", "z1"), namedViewDetail.getAuths());

        verify(readAccessPredicate).test(testUser, adminAuth);
        verify(writeAccessPredicate).test(testUser, adminAuth);
    }

    @Test
    public void shouldConfigureUnrestrictedAccessPredicateForReadAccessTestByDefault() {
        final NamedViewDetail namedViewDetail = createNamedViewDetailBuilder().build();
        assertEquals(
                new UnrestrictedAccessPredicate(),
                namedViewDetail.getReadAccessPredicate());
        assertTrue(namedViewDetail.getAuths().containsAll(asList("writeAuth1", "writeAuth2")));
    }

    @Test
    public void shouldConfigureDefaultAccessPredicateForWriteAccessTestByDefault() {
        final NamedViewDetail namedViewDetail = createNamedViewDetailBuilder().build();
        assertEquals(
                new NamedViewWriteAccessPredicate(new User.Builder().userId("creator").build(), asList("writeAuth1", "writeAuth2")),
                namedViewDetail.getWriteAccessPredicate());
        assertTrue(namedViewDetail.getAuths().containsAll(asList("writeAuth1", "writeAuth2")));
    }

    private NamedViewDetail.Builder createNamedViewDetailBuilder() {
        final Map<String, ViewParameterDetail> params = new HashMap<>();
        params.put("entityGroup", new ViewParameterDetail.Builder()
                .description("some description")
                .defaultValue("red")
                .valueClass(String.class)
                .build());
        return new NamedViewDetail.Builder()
                .name("view1")
                .creatorId("creator")
                .writers(asList("writeAuth1", "writeAuth2"))
                .description("description")
                .parameters(params)
                .view("{\"entities\": {" +
                        "\"${entityGroup}\":{}" +
                        "}" +
                        "}");
    }
}
