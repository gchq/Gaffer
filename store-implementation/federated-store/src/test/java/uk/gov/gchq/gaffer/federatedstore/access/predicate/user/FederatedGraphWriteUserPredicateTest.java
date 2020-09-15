package uk.gov.gchq.gaffer.federatedstore.access.predicate.user;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import static org.junit.jupiter.api.Assertions.*;

class FederatedGraphWriteUserPredicateTest {

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        FederatedGraphWriteUserPredicate predicate = new FederatedGraphWriteUserPredicate("userA");
        String json = "{" +
                "   \"class\": \"uk.gov.gchq.gaffer.federatedstore.access.predicate.user.FederatedGraphWriteUserPredicate\"," +
                "   \"creatingUserId\": \"userA\"" +
                "}";

        // When
        String serialised = new String(JSONSerialiser.serialise(predicate));
        FederatedGraphWriteUserPredicate deserialised = JSONSerialiser.deserialise(json, FederatedGraphWriteUserPredicate.class);

        // Then
        JsonAssert.assertEquals(json, serialised);
        assertEquals(predicate, deserialised);
    }
}