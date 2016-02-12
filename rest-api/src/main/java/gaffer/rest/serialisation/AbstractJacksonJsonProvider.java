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

package gaffer.rest.serialisation;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;

/**
 * A <code>JacksonJsonProvider</code> enables the automatic serialisation and deserialisation to/from JSON.
 * By default the JSON will not include nulls or default values.
 * To accept json in the rest api this class must be extended.
 * To register it as a provider add the class annotations: @Provider and @Produces(MediaType.APPLICATION_JSON).
 */
public abstract class AbstractJacksonJsonProvider extends JacksonJaxbJsonProvider {
    public AbstractJacksonJsonProvider() {
        super.setMapper(createMapper());
    }

    public AbstractJacksonJsonProvider(final ObjectMapper mapper) {
        super.setMapper(mapper);
    }

    protected ObjectMapper createMapper() {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
        return mapper;
    }
}
