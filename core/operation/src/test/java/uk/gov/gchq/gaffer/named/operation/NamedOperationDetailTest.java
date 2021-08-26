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

package uk.gov.gchq.gaffer.named.operation;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.access.ResourceType;
import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.user.CustomUserPredicate;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.user.User;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class NamedOperationDetailTest {

    @Test
    public void shouldBeNamedOperationResourceType() {
        assertEquals(ResourceType.NamedOperation, new NamedOperationDetail().getResourceType());
    }

    @Test
    public void shouldDefaultReadAccessPredicateIfNoneSpecified() {
        final List<String> readers = asList("readerAuth1", "readerAuth2");
        final NamedOperationDetail namedOperationDetail = getBaseNamedOperationDetailBuilder()
                .readers(readers)
                .build();
        assertEquals(
                new AccessPredicate(new User.Builder().userId("creatorUserId").build(), readers),
                namedOperationDetail.getOrDefaultReadAccessPredicate());
    }

    @Test
    public void shouldDefaultWriteAccessPredicateIfNoneSpecified() {
        final List<String> writers = asList("writerAuth1", "writerAuth2");
        final NamedOperationDetail namedOperationDetail = getBaseNamedOperationDetailBuilder()
                .writers(writers)
                .build();
        assertEquals(
                new AccessPredicate(new User.Builder().userId("creatorUserId").build(), writers),
                namedOperationDetail.getOrDefaultWriteAccessPredicate());
    }

    @Test
    public void shouldConfigureCustomReadAccessPredicateWhenSpecified() {
        final AccessPredicate customAccessPredicate = new AccessPredicate(new CustomUserPredicate());
        final NamedOperationDetail namedOperationDetail = getBaseNamedOperationDetailBuilder()
                .readAccessPredicate(customAccessPredicate)
                .build();
        assertEquals(customAccessPredicate, namedOperationDetail.getReadAccessPredicate());
    }

    @Test
    public void shouldMaintainOrderingAfterBeingEvaluated() {
        // Given
        NamedOperationDetail namedOperationDetail = getBaseNamedOperationDetailBuilder().writers(Arrays.asList("c", "b", "a"))
                .build();
        // When
        // Don't actually care about the result
        namedOperationDetail.hasWriteAccess(new User());

        // Then
        assertEquals(Arrays.asList("c", "b", "a"), namedOperationDetail.getWriteAccessRoles());
    }

    @Test
    public void shouldConfigureCustomWriteAccessPredicateWhenSpecified() {
        final AccessPredicate customAccessPredicate = new AccessPredicate(new CustomUserPredicate());
        final NamedOperationDetail namedOperationDetail = getBaseNamedOperationDetailBuilder()
                .writeAccessPredicate(customAccessPredicate)
                .build();
        assertEquals(customAccessPredicate, namedOperationDetail.getWriteAccessPredicate());
    }

    @Test
    public void shouldDeserialiseStringOpChain() throws SerialisationException {
        // Given
        String json = "{" +
                "   \"operationName\": \"operationName\"," +
                "   \"creatorId\": \"creatorUserId\"," +
                "   \"operations\": \"{\\\"operations\\\":[{\\\"class\\\":\\\"uk.gov.gchq.gaffer.store.operation.GetSchema\\\",\\\"compact\\\":false}]}\"" +
                "}";
        // When
        NamedOperationDetail deserialised = JSONSerialiser.deserialise(json, NamedOperationDetail.class);

        // Then
        assertEquals("{\"operations\":[{\"class\":\"uk.gov.gchq.gaffer.store.operation.GetSchema\",\"compact\":false}]}", deserialised.getOperations());
    }

    @Test
    public void shouldDeserialiseNormalOpChain() throws SerialisationException {
        // Given
        String json = "{" +
                "   \"operationName\": \"operationName\"," +
                "   \"creatorId\": \"creatorUserId\"," +
                "   \"operationChain\": {\"operations\":[{\"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\"}]}" +
                "}";
        // When
        NamedOperationDetail deserialised = JSONSerialiser.deserialise(json, NamedOperationDetail.class);

        // Then
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.operation.OperationChain\",\"operations\":[{\"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\"}]}", deserialised.getOperations());
    }

    @Test
    public void shouldSerialiseToStringOpChain() throws SerialisationException {
        // Given
        NamedOperationDetail nop = new NamedOperationDetail.Builder()
                .operationName("test")
                .operationChain(new OperationChain.Builder().first(new GetAllElements()).build())
                .build();

        // When
        String serialised = new String(JSONSerialiser.serialise(nop));

        // Then
        String expected =
                "{" +
                "   \"operationName\": \"test\"," +
                "   \"operations\": \"{\\\"class\\\":\\\"uk.gov.gchq.gaffer.operation.OperationChain\\\",\\\"operations\\\":[{\\\"class\\\":\\\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\\\"}]}\"" +
                "}";

        JsonAssert.assertEquals(expected, serialised);
    }

    @Test
    public void shouldSerialiseAndDeserialiseAccessPredicates() throws SerialisationException {
        NamedOperationDetail nop = new NamedOperationDetail.Builder()
                .operationName("test")
                .operationChain(new OperationChain.Builder().first(new GetAllElements()).build())
                .readAccessPredicate(new AccessPredicate("a", asList("b", "c")))
                .writeAccessPredicate(new AccessPredicate("x", asList("y", "z")))
                .build();

        // When
        String serialised = new String(JSONSerialiser.serialise(nop));

        // Then
        String expected =
                "{" +
                "   \"operationName\": \"test\"," +
                "   \"operations\": \"{\\\"class\\\":\\\"uk.gov.gchq.gaffer.operation.OperationChain\\\",\\\"operations\\\":[{\\\"class\\\":\\\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\\\"}]}\"," +
                "   \"readAccessPredicate\": {" +
                "       \"class\": \"uk.gov.gchq.gaffer.access.predicate.AccessPredicate\"," +
                "       \"userPredicate\": {" +
                "           \"class\": \"uk.gov.gchq.gaffer.access.predicate.user.DefaultUserPredicate\"," +
                "           \"creatingUserId\": \"a\"," +
                "           \"auths\": [\"b\", \"c\"]" +
                "       }" +
                "   }," +
                "   \"writeAccessPredicate\": {" +
                "       \"class\": \"uk.gov.gchq.gaffer.access.predicate.AccessPredicate\"," +
                "       \"userPredicate\": {" +
                "           \"class\": \"uk.gov.gchq.gaffer.access.predicate.user.DefaultUserPredicate\"," +
                "           \"creatingUserId\": \"x\"," +
                "           \"auths\": [\"y\", \"z\"]" +
                "       }" +
                "   }" +
                "}";

        JsonAssert.assertEquals(expected, serialised);
        final NamedOperationDetail deserialised = JSONSerialiser.deserialise(serialised, NamedOperationDetail.class);
        assertEquals(nop, deserialised);
    }

    @Test
    public void shouldSerialiseAndDeserialiseReadersAndWriters() throws SerialisationException {
        NamedOperationDetail nop = new NamedOperationDetail.Builder()
                .operationName("test")
                .operationChain(new OperationChain.Builder().first(new GetAllElements()).build())
                .readers(asList("b", "c"))
                .writers(asList("y", "z"))
                .build();

        // When
        String serialised = new String(JSONSerialiser.serialise(nop));

        // Then
        String expected =
                "{" +
                        "   \"operationName\": \"test\"," +
                        "   \"operations\": \"{\\\"class\\\":\\\"uk.gov.gchq.gaffer.operation.OperationChain\\\",\\\"operations\\\":[{\\\"class\\\":\\\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\\\"}]}\"," +
                        "   \"readAccessRoles\": [\"b\", \"c\"]," +
                        "   \"writeAccessRoles\": [\"y\", \"z\"]" +
                        "}";

        JsonAssert.assertEquals(expected, serialised);
        final NamedOperationDetail deserialised = JSONSerialiser.deserialise(serialised, NamedOperationDetail.class);
        assertEquals(nop, deserialised);
    }

    @Test
    public void shouldDeserialiseCustomAccessPredicate() throws SerialisationException {
        // Given
        String json = "{" +
                "   \"operationName\": \"operationName\"," +
                "   \"creatorId\": \"creatorUserId\"," +
                "   \"operationChain\": {\"operations\":[{\"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\"}]}," +
                "   \"readAccessPredicate\": {" +
                "       \"class\": \"uk.gov.gchq.gaffer.access.predicate.AccessPredicate\"," +
                "       \"userPredicate\": {" +
                "           \"class\": \"uk.gov.gchq.gaffer.access.predicate.user.CustomUserPredicate\"" +
                "       }" +
                "   }" +
                "}";

        // When
        NamedOperationDetail deserialised = JSONSerialiser.deserialise(json, NamedOperationDetail.class);

        // Then
        AccessPredicate expected = new AccessPredicate(new CustomUserPredicate());
        assertEquals(expected, deserialised.getReadAccessPredicate());
    }


    @Test
    public void shouldBeJavaSerialisable() throws IOException, ClassNotFoundException {
        // Given
        final AccessPredicate customAccessPredicate = new AccessPredicate(new CustomUserPredicate());
        final NamedOperationDetail namedOperationDetail = getBaseNamedOperationDetailBuilder()
                .readAccessPredicate(customAccessPredicate)
                .build();
        // When
        NamedOperationDetail deserialised = (NamedOperationDetail) deserialise(serialise(namedOperationDetail));

        // Then
        assertEquals(namedOperationDetail, deserialised);
    }

    private static byte[] serialise(Object obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj);
        return b.toByteArray();
    }

    private static Object deserialise(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return o.readObject();
    }

    private NamedOperationDetail.Builder getBaseNamedOperationDetailBuilder() {
        return new NamedOperationDetail.Builder()
                .operationName("operationName")
                .labels(asList("label1", "label2"))
                .inputType("inputType")
                .description("description")
                .creatorId("creatorUserId")
                .parameters(Collections.emptyMap())
                .operationChain("{\"operations\":[{\"class\":\"uk.gov.gchq.gaffer.store.operation.GetSchema\",\"compact\":false}]}")
                .score(1);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfBothReadAccessRolesAndReadAccessPredicateAreSupplied() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> getBaseNamedOperationDetailBuilder()
                        .readers(asList("readerAuth1", "readerAuth2"))
                        .readAccessPredicate(new AccessPredicate(new CustomUserPredicate()))
                        .build())
                .withMessageContaining("Only one of readers or readAccessPredicate should be supplied.");
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfBothWriteAccessRolesAndWriteAccessPredicateAreSupplied() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> getBaseNamedOperationDetailBuilder()
                        .writers(asList("writerAuth1", "writerAuth2"))
                        .writeAccessPredicate(new AccessPredicate(new CustomUserPredicate()))
                        .build())
                .withMessageContaining("Only one of writers or writeAccessPredicate should be supplied.");
    }
}
