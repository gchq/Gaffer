/*
 * Copyright 2017-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation;

import org.apache.commons.lang3.exception.CloneFailedException;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FederatedOperationTest {

    private static class TestFederatedOperation implements FederatedOperation {

        @Override
        public Operation shallowClone() throws CloneFailedException {
            return null;
        }

        @Override
        public Map<String, String> getOptions() {
            return null;
        }

        @Override
        public void setOptions(final Map<String, String> options) {

        }
    }

    @Test
    public void shouldReturnTrueWhenOpChainHasFederatedOps() {
        // Given
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetElements())
                .then(new DiscardOutput())
                .then(new TestFederatedOperation())
                .then(new GetElements())
                .build();

        // When
        final boolean result = FederatedOperation.hasFederatedOperations(opChain);

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldReturnFalseWhenOpChainDoesNotHaveFederatedOps() {
        // Given
        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetAdjacentIds())
                .then(new GetElements())
                .build();

        // When
        final boolean result = FederatedOperation.hasFederatedOperations(opChain);

        // Then
        assertFalse(result);
    }
}
