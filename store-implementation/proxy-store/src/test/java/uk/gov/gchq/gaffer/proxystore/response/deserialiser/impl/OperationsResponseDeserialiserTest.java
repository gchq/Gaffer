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

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;

import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

public class OperationsResponseDeserialiserTest {

    @Test
    public void shouldThrowSerialisationExceptionWhenResponseIsInvalid() {
        assertThatExceptionOfType(SerialisationException.class).isThrownBy(() -> new OperationsResponseDeserialiser().deserialise(""));
        assertThatExceptionOfType(SerialisationException.class).isThrownBy(() -> new OperationsResponseDeserialiser().deserialise("{}"));
        assertThatExceptionOfType(SerialisationException.class).isThrownBy(() -> new OperationsResponseDeserialiser().deserialise("Junk"));
    }

    @Test
    public void shouldSkipInvalidClassNamesInResponse() throws SerialisationException {
        final String jsonString = "[\n" +
                "  \"uk.gov.gchq.gaffer.operation.impl.add.AddElements\"," +
                "  \"uk.gov.gchq.not.a.valid.Class\"" +
                "]";

        final Set<Class<? extends Operation>> operationClasses = new OperationsResponseDeserialiser().deserialise(jsonString);
        assertIterableEquals(Collections.singleton(AddElements.class), operationClasses);
    }

    @Test
    public void shouldSkipOperationsNotOnTheLocalClasspathInResponse() throws SerialisationException {
        /* Federated Store Operations are not accessible from the proxy-store module */
        final String jsonString = "[\n" +
                "  \"uk.gov.gchq.gaffer.operation.impl.add.AddElements\"," +
                "  \"uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds\"" +
                "]";

        final Set<Class<? extends Operation>> operationClasses = new OperationsResponseDeserialiser().deserialise(jsonString);
        assertIterableEquals(Collections.singleton(AddElements.class), operationClasses);
    }

    @Test
    public void shouldSkipClassNamesNotAssignableToOperationInResponse() throws SerialisationException {
        final String jsonString = "[\n" +
                "  \"uk.gov.gchq.gaffer.operation.impl.add.AddElements\"," +
                "  \"java.lang.String\"" +
                "]";

        final Set<Class<? extends Operation>> operationClasses = new OperationsResponseDeserialiser().deserialise(jsonString);
        assertIterableEquals(Collections.singleton(AddElements.class), operationClasses);
    }
}
