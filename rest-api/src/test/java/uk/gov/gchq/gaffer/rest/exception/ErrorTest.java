/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

import static org.junit.Assert.assertEquals;
import static uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser.createDefaultMapper;

public class ErrorTest {

    private final static String SIMPLE_MESSAGE = "Simple error message.";
    private final static String DETAIL_MESSAGE = "Detail error message.";
    private final static int STATUS_CODE = 500;

    @Test
    public void shouldSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final Error error = new Error.ErrorBuilder().simpleMessage(SIMPLE_MESSAGE)
                                                    .detailMessage(DETAIL_MESSAGE)
                                                    .statusCode(STATUS_CODE)
                                                    .build();

        final JSONSerialiser serialiser = new JSONSerialiser();

        // When
        final byte[] serialisedError = serialiser.serialise(error);
        final Error deserialisedError = serialiser.deserialise(serialisedError, error
                .getClass());

        // Then
        assertEquals(error, deserialisedError);
    }

    @Test
    public void shouldSerialiseToJson() throws JsonProcessingException {
        // Given
        final Error error = new Error.ErrorBuilder().simpleMessage(SIMPLE_MESSAGE)
                                                    .detailMessage(DETAIL_MESSAGE)
                                                    .statusCode(STATUS_CODE)
                                                    .build();

        final ObjectMapper mapper = createDefaultMapper();

        // When
        byte[] json = mapper.writeValueAsBytes(error);

        // Then
        JsonUtil.assertEquals(String.format("{" +
                "   \"statusCode\":500,%n" +
                "   \"status\":\"Internal Server Error\",%n" +
                "   \"simpleMessage\":\"Simple error message.\",%n" +
                "   \"detailMessage\":\"Detail error message.\"%n" +
                "}"), new String(json));
    }

}
