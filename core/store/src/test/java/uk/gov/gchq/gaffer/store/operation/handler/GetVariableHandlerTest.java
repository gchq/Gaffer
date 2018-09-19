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
import uk.gov.gchq.gaffer.operation.impl.GetVariable;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class GetVariableHandlerTest {
    private final String varName = "varName";
    private final String varVal = "varVal";
    private final Store store = mock(Store.class);

    @Test
    public void shouldGetVariableWhenExists() throws OperationException {
        final Context context = mock(Context.class);
        final GetVariableHandler handler = new GetVariableHandler();
        final GetVariable op = new GetVariable.Builder().variableName(varName).build();

        given(context.getVariable(varName)).willReturn(varVal);

        final Object variableValueFromOp = handler.doOperation(op, context, store);

        assertEquals(varVal, variableValueFromOp);
    }

    @Test
    public void shouldReturnNullWhenVariableDoesntExist() throws OperationException {
        final Context context = mock(Context.class);
        final GetVariableHandler handler = new GetVariableHandler();
        final GetVariable op = new GetVariable.Builder().variableName(varName).build();

        given(context.getVariable(varName)).willReturn(null);

        final Object variableValueFromOp = handler.doOperation(op, context, store);

        assertNull(variableValueFromOp);
    }
}
