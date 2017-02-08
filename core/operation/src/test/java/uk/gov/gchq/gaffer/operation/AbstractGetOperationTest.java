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
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.GetOperationImpl;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;


public class AbstractGetOperationTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    public void shouldCopyFieldsFromGivenOperationWhenConstructing() {
        // Given
        final GetOperation<ElementSeed, ?> operationToCopy = mock(GetOperation.class);
        final View view = mock(View.class);
        final GetOperation.IncludeEdgeType includeEdges = GetOperation.IncludeEdgeType.ALL;
        final boolean includeEntities = true;
        final boolean populateProperties = true;
        final CloseableIterable<ElementSeed> input = mock(CloseableIterable.class);

        given(operationToCopy.getView()).willReturn(view);
        given(operationToCopy.getInput()).willReturn(input);

        // When
        final GetOperation<ElementSeed, Element> operation = new GetOperationImpl<>(operationToCopy);

        // Then
        assertSame(view, operation.getView());
        assertSame(input, operation.getInput());
    }

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final String identifier = "identifier";
        final ElementSeed input = new EntitySeed(identifier);
        final GetOperationImpl<ElementSeed, Element> op = new GetOperationImpl<>(Collections.singletonList(input));

        // When
        byte[] json = serialiser.serialise(op, true);
        final GetOperationImpl<ElementSeed, Element> deserialisedOp = serialiser.deserialise(json, GetOperationImpl.class);

        // Then
        assertNotNull(deserialisedOp);
        assertEquals(identifier, ((EntitySeed) deserialisedOp.getInput().iterator().next()).getVertex());
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        //GetOperationImpl is a test object and has no builder.
    }
}
