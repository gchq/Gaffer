/*
 * Copyright 2018-2021 Crown Copyright
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
import uk.gov.gchq.koryphe.impl.function.CallMethod;
import uk.gov.gchq.koryphe.impl.predicate.CollectionContains;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.predicate.AdaptedPredicate;
import uk.gov.gchq.koryphe.util.JsonSerialiser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NamedViewDetailTest {
    private static final AccessPredicate READ_ACCESS_PREDICATE = new AccessPredicate(new CustomUserPredicate());
    private static final AccessPredicate WRITE_ACCESS_PREDICATE = new AccessPredicate(new CustomUserPredicate());

    @Test
    public void shouldCreatePredicatesIfNotSpecifiedInJson() throws IOException {
        // Given
        final String json = String.format("{%n" +
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
                "  \"readAccessPredicate\" : {%n" +
                "       \"class\" : \"uk.gov.gchq.gaffer.access.predicate.AccessPredicate\",%n" +
                "       \"userPredicate\" : {%n" +
                "           \"class\" : \"uk.gov.gchq.gaffer.access.predicate.user.CustomUserPredicate\"%n" +
                "       }%n" +
                "   }%n" +
                "}%n");

        // When
        NamedViewDetail deserialised = JsonSerialiser.deserialise(json, NamedViewDetail.class);

        // Then
        AccessPredicate expected = new NamedViewWriteAccessPredicate("creator", asList("writeAuth1", "writeAuth2"));
        assertEquals(expected, deserialised.getOrDefaultWriteAccessPredicate());
    }

    @Test
    public void shouldJsonSerialiseUsingAccessPredicates() throws SerialisationException {
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
                "  \"parameters\" : {%n" +
                "    \"entityGroup\" : {%n" +
                "      \"description\" : \"some description\",%n" +
                "      \"defaultValue\" : \"red\",%n" +
                "      \"valueClass\" : \"java.lang.String\",%n" +
                "      \"required\" : false%n" +
                "    }%n" +
                "  },%n" +
                "  \"view\" : \"{\\\"entities\\\": {\\\"${entityGroup}\\\":{}}}\",%n" +
                "  \"readAccessPredicate\" : {%n" +
                "       \"class\" : \"uk.gov.gchq.gaffer.access.predicate.AccessPredicate\",%n" +
                "       \"userPredicate\" : {%n" +
                "           \"class\" : \"uk.gov.gchq.gaffer.access.predicate.user.CustomUserPredicate\"%n" +
                "       }%n" +
                "   },%n" +
                "   \"writeAccessPredicate\" : {%n" +
                "       \"class\" : \"uk.gov.gchq.gaffer.access.predicate.AccessPredicate\",%n" +
                "       \"userPredicate\" : {%n" +
                "           \"class\" : \"uk.gov.gchq.gaffer.access.predicate.user.CustomUserPredicate\"%n" +
                "       }%n " +
                "   }%n" +
                "},%n");
        JsonAssert.assertEquals(expected, new String(json));

        final NamedViewDetail deserialisedNamedViewDetail = JSONSerialiser.deserialise(new String(json), NamedViewDetail.class);

        assertEquals(namedViewDetail, deserialisedNamedViewDetail);
    }

    @Test
    public void shouldJsonSerialiseUsingWriters() throws SerialisationException {
        // Given
        final NamedViewDetail namedViewDetail = createNamedViewDetailBuilder()
                .writers(asList("writeAuth1", "writeAuth2"))
                .build();

        // When
        final byte[] json = JSONSerialiser.serialise(namedViewDetail, true);

        // Then
        final String expected = String.format("{%n" +
                "  \"name\" : \"view1\",%n" +
                "  \"description\" : \"description\",%n" +
                "  \"creatorId\" : \"creator\",%n" +
                "  \"writeAccessRoles\" : [\"writeAuth1\", \"writeAuth2\"],%n" +
                "  \"parameters\" : {%n" +
                "    \"entityGroup\" : {%n" +
                "      \"description\" : \"some description\",%n" +
                "      \"defaultValue\" : \"red\",%n" +
                "      \"valueClass\" : \"java.lang.String\",%n" +
                "      \"required\" : false%n" +
                "    }%n" +
                "  },%n" +
                "  \"view\" : \"{\\\"entities\\\": {\\\"${entityGroup}\\\":{}}}\"%n" +
                "}%n");
        JsonAssert.assertEquals(expected, new String(json));

        final NamedViewDetail deserialisedNamedViewDetail = JSONSerialiser.deserialise(new String(json), NamedViewDetail.class);

        assertEquals(namedViewDetail, deserialisedNamedViewDetail);
    }

    @Test
    public void shouldTestAccessUsingCustomAccessPredicatesWhenConfigured() {
        // Given
        final User testUser = new User.Builder().userId("testUserId").build();
        final User differentUser = new User.Builder().userId("differentUserId").opAuth("different").build();
        final String adminAuth = "adminAuth";
        final AccessPredicate readAccessPredicate = new AccessPredicate(new AdaptedPredicate(new CallMethod("getUserId"), new IsEqual("testUserId")));
        final AccessPredicate writeAccessPredicate = new AccessPredicate(new AdaptedPredicate(new CallMethod("getOpAuths"), new CollectionContains("different")));

        // When
        final NamedViewDetail namedViewDetail = createNamedViewDetailBuilder()
                .readAccessPredicate(readAccessPredicate)
                .writeAccessPredicate(writeAccessPredicate)
                .build();

        // Then
        assertTrue(namedViewDetail.hasReadAccess(testUser, adminAuth));
        assertFalse(namedViewDetail.hasReadAccess(differentUser, adminAuth));
        assertFalse(namedViewDetail.hasWriteAccess(testUser, adminAuth));
        assertTrue(namedViewDetail.hasWriteAccess(differentUser, adminAuth));
    }

    @Test
    public void shouldConfigureUnrestrictedAccessPredicateForReadAccessTestByDefault() {
        final NamedViewDetail namedViewDetail = createNamedViewDetailBuilder().build();
        assertEquals(
                new UnrestrictedAccessPredicate(),
                namedViewDetail.getOrDefaultReadAccessPredicate());
    }

    @Test
    public void shouldConfigureDefaultAccessPredicateForWriteAccessTestByDefault() {
        final List<String> writers = asList("writeAuth1", "writeAuth2");
        final NamedViewDetail namedViewDetail = createNamedViewDetailBuilder()
                .writers(writers)
                .build();
        assertEquals(
                new NamedViewWriteAccessPredicate(new User.Builder().userId("creator").build(), writers),
                namedViewDetail.getOrDefaultWriteAccessPredicate());
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
                .description("description")
                .parameters(params)
                .view("{\"entities\": {" +
                        "\"${entityGroup}\":{}" +
                        "}" +
                        "}");
    }

    @Test
    public void shouldBeSerialisable() throws IOException, ClassNotFoundException {
        // Given
        NamedViewDetail namedView = new NamedViewDetail.Builder()
                .creatorId("creator")
                .name("test")
                .view(new View.Builder()
                        .globalElements(new GlobalViewElementDefinition.Builder()
                            .groupBy()
                            .build())
                        .build())
                .writers(asList("a", "b", "c"))
                .build();

        // When
        byte[] serialized = serialize(namedView);
        Object deserialized = deserialize(serialized);

        // Then
        assertEquals(namedView, deserialized);
    }


    @Test
    public void shouldMaintainOrderingAfterBeingEvaluated() {
        // Given
        NamedViewDetail namedViewDetail = new NamedViewDetail.Builder()
                .name("view")
                .view(new View())
                .writers(Arrays.asList("c", "b", "a"))
                .build();
        // When
        // Don't actually care about the result
        namedViewDetail.hasWriteAccess(new User());

        // Then
        assertEquals(Arrays.asList("c", "b", "a"), namedViewDetail.getWriteAccessRoles());
    }

    @Test
    public void shouldBeAbleToDetermineIfAUserHasAccessAfterBeingDeserialised() throws IOException, ClassNotFoundException {
        // Given
        NamedViewDetail namedView = new NamedViewDetail.Builder()
                .creatorId("creator")
                .name("test")
                .view(new View.Builder()
                        .globalElements(new GlobalViewElementDefinition.Builder()
                                .groupBy()
                                .build())
                        .build())
                .writers(asList("a", "b", "c"))
                .build();

        // When
        byte[] serialized = serialize(namedView);
        NamedViewDetail deserialized = (NamedViewDetail) deserialize(serialized);

        // Then
        assertTrue(deserialized.hasReadAccess(new User("someone")));
        assertFalse(deserialized.hasWriteAccess(new User("someone")));
    }


    private static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj);
        return b.toByteArray();
    }

    private static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return o.readObject();
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfBothWritersAndWriteAccessPredicateAreSupplied() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new NamedViewDetail.Builder()
                        .creatorId("creator")
                        .name("test")
                        .view(new View.Builder()
                                .globalElements(new GlobalViewElementDefinition.Builder()
                                        .groupBy()
                                        .build())
                                .build())
                        .writers(asList("a", "b", "c"))
                        .writeAccessPredicate(new AccessPredicate(new CustomUserPredicate()))
                        .build())
                .withMessageContaining("Only one of writers or writeAccessPredicate should be supplied.");
    }
}
