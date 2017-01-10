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

package uk.gov.gchq.gaffer.rest.serialisation;

import com.fasterxml.jackson.databind.ObjectMapper;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import static uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser.createDefaultMapper;

/**
 * A {@link javax.ws.rs.ext.ContextResolver} implementation to provide a default
 * {@link com.fasterxml.jackson.databind.ObjectMapper} for converting objects to
 * JSON.
 */
@Provider
public class JacksonJsonProvider implements ContextResolver<ObjectMapper> {

    public final ObjectMapper mapper;

    public JacksonJsonProvider() {
        this.mapper = createDefaultMapper();
    }

    @Override
    public ObjectMapper getContext(final Class<?> aClass) {
        return mapper;
    }
}
