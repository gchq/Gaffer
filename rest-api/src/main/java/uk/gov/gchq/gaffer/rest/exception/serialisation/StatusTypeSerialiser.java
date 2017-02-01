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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.commons.lang3.text.WordUtils;
import javax.ws.rs.core.Response.StatusType;
import java.io.IOException;

public class StatusTypeSerialiser extends JsonSerializer<StatusType> {

    @Override
    public void serialize(final StatusType statusType, final JsonGenerator generator, final SerializerProvider provider) throws IOException, JsonProcessingException {
        final String statusStr = statusType.toString().replace('_', ' ');

        generator.writeString(WordUtils.capitalize(statusStr));
    }
}
