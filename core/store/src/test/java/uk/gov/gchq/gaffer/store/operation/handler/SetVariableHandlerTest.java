/*
 * Copyright 2017-2021 Crown Copyright
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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SetVariable;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.user.User;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class SetVariableHandlerTest {

    @Test
    public void shouldSetVariableInContext() throws OperationException {
        // Given
        final Context context = new Context(new User());
        final Store store = mock(Store.class);
        final String testVarName = "testVarName";
        final int testVarValue = 4;

        SetVariableHandler handler = new SetVariableHandler();
        SetVariable op = new SetVariable.Builder().variableName(testVarName).input(testVarValue).build();

        // When
        handler.doOperation(op, context, store);

        // Then
        assertTrue(context.getVariable(testVarName).equals(testVarValue));
        assertTrue(context.getVariables().equals(ImmutableMap.of(testVarName, testVarValue)));
    }

    @Test
    public void shouldThrowExceptionWithNullVariableKey() throws OperationException {
        // Given
        final Context context = new Context(new User());
        final Store store = mock(Store.class);

        SetVariableHandler handler = new SetVariableHandler();
        SetVariable op = new SetVariable();

        // When / Then
        assertThatIllegalArgumentException().isThrownBy(() -> handler.doOperation(op, context, store)).withMessage("Variable name cannot be null");
    }

    @Test
    public void shouldNotAllowNullInputVariableToBeAdded() throws OperationException {
        // Given
        final Context context = new Context(new User());
        final Store store = mock(Store.class);
        final String testVarName = "testVarName";
        final Object testVarValue = null;

        SetVariableHandler handler = new SetVariableHandler();
        SetVariable op = new SetVariable.Builder().variableName(testVarName).input(testVarValue).build();

        // When / Then
        assertThatIllegalArgumentException().isThrownBy(() -> handler.doOperation(op, context, store)).withMessage("Variable input value cannot be null");
    }

    @Test
    public void setTwoVarsWithoutFailure() throws OperationException {
        // Given
        final Context context = new Context(new User());
        final Store store = mock(Store.class);
        final String varName = "testVarName";
        final String varVal = "varVal";
        final String varName1 = "testVarName1";
        final String varVal1 = "varVal1";

        SetVariableHandler handler = new SetVariableHandler();
        SetVariable op = new SetVariable.Builder()
                .variableName(varName)
                .input(varVal)
                .build();
        SetVariable op1 = new SetVariable.Builder()
                .variableName(varName1)
                .input(varVal1)
                .build();

        // When
        handler.doOperation(op, context, store);
        handler.doOperation(op1, context, store);

        // Then
        assertEquals(2, context.getVariables().size());
        assertEquals(ImmutableMap.of(varName, varVal, varName1, varVal1), context.getVariables());
    }
}
