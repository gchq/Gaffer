/*
 * Copyright 2016-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameCache;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

/**
 * A {@link javax.ws.rs.ext.ContextResolver} implementation to provide the
 * {@link ObjectMapper} from {@link JSONSerialiser}. The JSONSerialiser and
 * ObjectMapper can be configured by System Properties, see {@link JSONSerialiser}
 */
@Provider
public class RestJsonProvider implements ContextResolver<ObjectMapper> {
    public RestJsonProvider() {
        SimpleClassNameCache.initialise();
        JSONSerialiser.update();
    }

    @Override
    public ObjectMapper getContext(final Class<?> aClass) {
        return JSONSerialiser.getMapper();
    }
}
