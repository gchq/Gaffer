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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain.Builder;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.OperationImpl;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetRelatedEdges;
import uk.gov.gchq.gaffer.operation.impl.get.GetRelatedElements;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OperationChainTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    public void shouldSerialiseAndDeserialiseOperationChain() throws SerialisationException {
        // Given
        final OperationChain<Object> opChain = new Builder()
                .first(new OperationImpl<>())
                .then(new OperationImpl<>())
                .build();

        // When
        byte[] json = serialiser.serialise(opChain, true);
        final OperationChain deserialisedOp = serialiser.deserialise(json, OperationChain.class);

        // Then
        assertNotNull(deserialisedOp);
        assertEquals(2, deserialisedOp.getOperations().size());
        assertEquals(OperationImpl.class, deserialisedOp.getOperations().get(0).getClass());
        assertEquals(OperationImpl.class, deserialisedOp.getOperations().get(1).getClass());
    }

    @Test
    public void shouldBuildOperationChain() {
        // Given
        final AddElements addElements = mock(AddElements.class);
        final GetAdjacentEntitySeeds getAdj1 = mock(GetAdjacentEntitySeeds.class);
        final GetAdjacentEntitySeeds getAdj2 = mock(GetAdjacentEntitySeeds.class);
        final GetRelatedElements<EntitySeed, Element> getRelElements = mock(GetRelatedElements.class);

        // When
        final OperationChain<CloseableIterable<Element>> opChain = new Builder()
                .first(addElements)
                .then(getAdj1)
                .then(getAdj2)
                .then(getRelElements)
                .build();

        // Then
        assertArrayEquals(new Operation[]{
                        addElements,
                        getAdj1,
                        getAdj2,
                        getRelElements},
                opChain.getOperationArray());
    }

    @Test
    public void shouldReturnReadableStringForToString() {
        // Given
        final AddElements addElements = new AddElements();
        final GetAdjacentEntitySeeds getAdj1 = new GetAdjacentEntitySeeds();
        final GetAdjacentEntitySeeds getAdj2 = new GetAdjacentEntitySeeds();
        final GetRelatedElements<EntitySeed, Element> getRelElements = new GetRelatedElements<>();
        final OperationChain<CloseableIterable<Element>> opChain = new Builder()
                .first(addElements)
                .then(getAdj1)
                .then(getAdj2)
                .then(getRelElements)
                .build();

        // When
        final String toString = opChain.toString();

        // Then
        final String expectedToString =
                "OperationChain[AddElements->GetAdjacentEntitySeeds->GetAdjacentEntitySeeds->GetRelatedElements]";
        assertEquals(expectedToString, toString);
    }

    @Test
    public void shouldBuildOperationChainWithSingleOperation() throws SerialisationException {
        // Given
        final GetAdjacentEntitySeeds getAdjacentEntitySeeds = mock(GetAdjacentEntitySeeds.class);

        // When
        final OperationChain opChain = new OperationChain.Builder()
                .first(getAdjacentEntitySeeds)
                .build();

        // Then
        assertEquals(1, opChain.getOperations().size());
        assertSame(getAdjacentEntitySeeds, opChain.getOperations().get(0));
    }

    @Test
    public void shouldBuildOperationChain_AdjEntitySeedsThenRelatedEdges() throws SerialisationException {
        // Given
        final GetAdjacentEntitySeeds getAdjacentEntitySeeds = mock(GetAdjacentEntitySeeds.class);
        final GetRelatedEdges<EntitySeed> getRelatedEdges = mock(GetRelatedEdges.class);

        // When
        final OperationChain opChain = new OperationChain.Builder()
                .first(getAdjacentEntitySeeds)
                .then(getRelatedEdges)
                .build();

        // Then
        assertEquals(2, opChain.getOperations().size());
        assertSame(getAdjacentEntitySeeds, opChain.getOperations().get(0));
        assertSame(getRelatedEdges, opChain.getOperations().get(1));
    }
}
