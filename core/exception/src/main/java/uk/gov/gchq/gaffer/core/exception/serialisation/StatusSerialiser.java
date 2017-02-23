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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.commons.lang3.text.WordUtils;
import uk.gov.gchq.gaffer.core.exception.Status;
import java.io.IOException;

/**
 * Custom serialiser to format the HTTP status string when displaying error
 * messages to users.
 */
public class StatusSerialiser extends JsonSerializer<Status> {

    @Override
    public void serialize(final Status statusType, final JsonGenerator generator,
            final SerializerProvider provider) throws IOException, JsonProcessingException {
        final String statusStr = statusType.toString().replace('_', ' ');

        generator.writeString(WordUtils.capitalize(statusStr));
    }
}
