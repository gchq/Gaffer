/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.operation;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.impl.OperationImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class AbstractOperationTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    public void shouldCopyFieldsFromGivenOperationWhenConstructing() {
        // Given
        final Operation<String, ?> operationToCopy = mock(Operation.class);
        final View view = mock(View.class);
        final String input = "input value";

        given(operationToCopy.getView()).willReturn(view);
        given(operationToCopy.getInput()).willReturn(input);

        // When
        final Operation<String, String> operation = new OperationImpl<>(operationToCopy);

        // Then
        assertSame(view, operation.getView());
        assertSame(input, operation.getInput());
    }

    @Test
    public void shouldCastToGenericType() {
        // Given
        Object result = "the result";
        final Operation<String, String> operation = new OperationImpl<>();

        // When
        String castedResult = operation.castToOutputType(result);

        // Then
        assertSame(result, castedResult);
    }


    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final String input = "some input";
        final OperationImpl<String, String> op = new OperationImpl<>(input);

        // When
        byte[] json = serialiser.serialise(op, true);
        final OperationImpl deserialisedOp = serialiser.deserialise(json, OperationImpl.class);

        // Then
        assertNotNull(deserialisedOp);
        assertEquals(input, deserialisedOp.getInput());
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        //Pass Test Operation has no builder
    }
}

