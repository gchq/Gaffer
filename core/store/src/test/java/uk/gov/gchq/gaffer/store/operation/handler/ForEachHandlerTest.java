/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler;

import org.junit.Test;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.ForEach;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.user.User;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class ForEachHandlerTest {

    private final Store store = mock(Store.class);
    private final Context context = new Context(new User());

    @Test
    public void shouldExecuteForEachOperationOnInputWithValidResults() {

    }

    @Test
    public void shouldExecuteForEachOperationOnInputWithEmptyIterable() {

    }

    @Test
    public void shouldThrowExceptionWithNullOperation() {
        // Given
        final ForEach op = new ForEach.Builder<>()
                .operation(null)
                .build();
        final ForEachHandler handler = new ForEachHandler();

        // When / Then
        try {
            handler.doOperation(op, context, store);
            fail("Exception expected");
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains("Operation cannot be null"));
        }
    }

    @Test
    public void shouldThrowExceptionWithNullInput() {
        // Given
        final ForEach op = new ForEach.Builder<>()
                .input(null)
                .build();
        final ForEachHandler handler = new ForEachHandler();

        // When / Then
        try {
            handler.doOperation(op, context, store);
            fail("Exception expected");
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains("Input cannot be null"));
        }
    }

    @Test
    public void shouldReturnEmptyIterableWithOperationThatDoesntImplementOutput() {

    }

    @Test
    public void shouldReturnEmptyIterableWithOperationThatDoesntImplementInput() {

    }
}
