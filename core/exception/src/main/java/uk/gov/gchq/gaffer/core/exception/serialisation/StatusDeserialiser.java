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
package uk.gov.gchq.gaffer.core.exception.serialisation;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.core.exception.Status;
import java.io.IOException;

/**
 * Custom deserialiser for handling formatted {@link uk.gov.gchq.gaffer.core.exception.Status}
 * representations.
 */
public class StatusDeserialiser extends JsonDeserializer<Status> {

    @SuppressFBWarnings("DM_CONVERT_CASE")
    @Override
    public Status deserialize(final JsonParser jsonParser,
            final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {

        final ObjectCodec codec = jsonParser.getCodec();
        final JsonNode node = codec.readTree(jsonParser);

        final String statusStr = node.asText()
                                     .toUpperCase()
                                     .replace(' ', '_');

        return Status.valueOf(statusStr);
    }
}
