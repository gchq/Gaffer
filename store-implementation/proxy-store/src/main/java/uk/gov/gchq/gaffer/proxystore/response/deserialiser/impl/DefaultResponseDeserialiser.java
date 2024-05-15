/*
 * Copyright 2021-2024 Crown Copyright
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.gaffer.proxystore.response.deserialiser.ResponseDeserialiser;

public class DefaultResponseDeserialiser<O> implements ResponseDeserialiser<O> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultResponseDeserialiser.class);

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
            // For some operations like 'Limit' we may not know the output type up front so it
            // will be left as a generic '?', this means some objects will not be deserialised correctly.
            try {
                // Check if we can deserialise to common outputs like list of elements.
                // This is a workaround until proxy store has been refactored
                if (typeReference.getType().getTypeName().equals(Iterable.class.getName() + "<?>")) {
                    return (O) JSONSerialiser.deserialise(encodeString(jsonString), new TypeReferenceImpl.IterableElement());
                } else {
                    return JSONSerialiser.deserialise(encodeString(jsonString), typeReference);
                }
            } catch (final SerialisationException e) {
                // The input is likely a valid JSON value type, so process using the deserialiser
                LOGGER.error("Unable to deserialse Iterable<?> as Iterable<Elements> using default deserialisation", e);
                return JSONSerialiser.deserialise(encodeString(jsonString), typeReference);
            }
        }
    }
}
