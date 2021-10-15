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

package uk.gov.gchq.gaffer.rest.serialisation;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.koryphe.function.KorypheFunction;
import uk.gov.gchq.koryphe.impl.function.ToString;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ObjectMapperProviderTest {

    @Test
    public void shouldResolveFullClassNames() throws IOException {
        // When
        ObjectMapper objectMapper = new ObjectMapperProvider().getObjectMapper();

        String json = "{" +
                "\"class\": \"uk.gov.gchq.koryphe.impl.function.ToString\"" +
                "}";

        // Then
        Object deserialised = objectMapper.readValue(json, KorypheFunction.class);
        assertEquals(ToString.class, deserialised.getClass());
    }

    @Test
    public void shouldResolveShortClassNames() throws IOException {
        // When
        ObjectMapper objectMapper = new ObjectMapperProvider().getObjectMapper();

        String json = "{" +
                "\"class\": \"ToString\"" +
                "}";

        // Then
        Object deserialised = objectMapper.readValue(json, KorypheFunction.class);
        assertEquals(ToString.class, deserialised.getClass());
    }
}
