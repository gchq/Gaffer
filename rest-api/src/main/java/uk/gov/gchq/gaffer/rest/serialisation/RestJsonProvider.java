/*
 * Copyright 2016 Crown Copyright
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
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.Provider;

/**
 * A <code>RestJsonProvider</code> enables the automatic serialisation and deserialisation to/from JSON.
 * By default the JSON will not include nulls.
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
public class RestJsonProvider extends JacksonJaxbJsonProvider {
    public RestJsonProvider() {
        super.setMapper(createMapper());
    }

    public RestJsonProvider(final ObjectMapper mapper) {
        super.setMapper(mapper);
    }

    protected ObjectMapper createMapper() {
        return JSONSerialiser.createDefaultMapper();
    }
}
