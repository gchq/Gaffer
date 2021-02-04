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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.proxystore.response.deserialiser.ResponseDeserialiser;

import java.util.HashSet;
import java.util.Set;

public class OperationsResponseDeserialiser implements ResponseDeserialiser<Set<Class<? extends Operation>>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OperationsResponseDeserialiser.class);

    @Override
    public Set<Class<? extends Operation>> deserialise(final String jsonString) throws SerialisationException {
        final byte[] bytes = encodeString(jsonString);
        final Set<?> operationClassNamesSet = JSONSerialiser.deserialise(bytes, new TypeReference<Set>() {
        });
        final Set<Class<? extends Operation>> operationClasses = new HashSet<>();
        for (Object operationClassName : operationClassNamesSet) {
            try {
                final Class operationClass = Class.forName(operationClassName.toString());
                if (Operation.class.isAssignableFrom(operationClass)) {
                    operationClasses.add(operationClass);
                } else {
                    LOGGER.warn("{} is not assignable to {} - it will be ignored.", operationClass, Operation.class.getName());
                }
            } catch (final ClassNotFoundException classNotFoundException) {
                LOGGER.warn("Could not find Class for {}, received exception: {}. It will be ignored.", operationClassName, classNotFoundException.getMessage());
            }
        }
        return operationClasses;
    }
}
