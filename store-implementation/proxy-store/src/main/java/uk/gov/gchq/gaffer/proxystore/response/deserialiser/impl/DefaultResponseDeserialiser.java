/*
 * Copyright 2021 Crown Copyright
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
package uk.gov.gchq.gaffer.proxystore.response.deserialiser.impl;

import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.proxystore.response.deserialiser.ResponseDeserialiser;

public class DefaultResponseDeserialiser<O> implements ResponseDeserialiser<O> {

    private final TypeReference<O> typeReference;

    public DefaultResponseDeserialiser(final TypeReference<O> typeReference) {
        this.typeReference = typeReference;
    }

    @Override
    public O deserialise(final String jsonString) throws SerialisationException {
        // Special handling for String values returned while using the ProxyStore
        if (typeReference.getType().equals(Object.class) && !jsonString.matches("^(-?\\d*\\.?\\d*|false|true|null|\\[.*\\]|\\{.*\\})$")) {
            // The input is likely a plain java.lang.String object, so return as-is
            return (O) jsonString;
        } else {
            // The input is likely a valid JSON value type, so process using the deserialiser
            return JSONSerialiser.deserialise(encodeString(jsonString), typeReference);
        }
    }
}
