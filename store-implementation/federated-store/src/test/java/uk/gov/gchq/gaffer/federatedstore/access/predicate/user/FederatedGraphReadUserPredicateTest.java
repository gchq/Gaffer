package uk.gov.gchq.gaffer.federatedstore.access.predicate.user;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

class FederatedGraphReadUserPredicateTest {

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        FederatedGraphReadUserPredicate federatedGraphReadUserPredicate = new FederatedGraphReadUserPredicate("user", Arrays.asList("myAuth"), true);
        String json = "{" +
                "   \"class\": \"uk.gov.gchq.gaffer.federatedstore.access.predicate.user.FederatedGraphReadUserPredicate\"," +
                "   \"creatingUserId\": \"user\"," +
                "   \"auths\": [ \"myAuth\" ]," +
                "   \"public\": true" +
                "}";

        // When
        String serialised = new String(JSONSerialiser.serialise(federatedGraphReadUserPredicate));
        FederatedGraphReadUserPredicate deserialised = JSONSerialiser.deserialise(json, FederatedGraphReadUserPredicate.class);

        // Then
        JsonAssert.assertEquals(json, serialised);
        assertEquals(federatedGraphReadUserPredicate, deserialised);
    }
}