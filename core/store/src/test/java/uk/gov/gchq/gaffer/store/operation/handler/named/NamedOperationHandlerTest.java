/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.named;


import org.junit.Test;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.operation.OperationException;

import static org.junit.Assert.assertTrue;

public class NamedOperationHandlerTest {
    @Test
    public void shouldTest() throws OperationException {
        // Given
        final NamedOperationHandler handler = new NamedOperationHandler();
        final NamedOperation<?, Object> op = new NamedOperation.Builder<>()
                .name("opName")
                .build();

        // When / Then
        try {
            handler.doOperation(op, null, null);
        } catch (final UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains(op.getOperationName()));
        }

    }
}
