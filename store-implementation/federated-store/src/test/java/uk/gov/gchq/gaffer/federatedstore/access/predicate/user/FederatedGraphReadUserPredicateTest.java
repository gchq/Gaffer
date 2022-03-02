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

package uk.gov.gchq.gaffer.federatedstore.access.predicate.user;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(deserialised).isEqualTo(federatedGraphReadUserPredicate);
    }
}
