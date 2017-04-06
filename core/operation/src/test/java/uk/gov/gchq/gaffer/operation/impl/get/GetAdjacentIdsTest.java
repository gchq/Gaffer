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

package uk.gov.gchq.gaffer.operation.impl.get;

import org.junit.Test;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters.DirectedType;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import java.util.Iterator;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class GetAdjacentIdsTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    public void shouldSetDirectedTypeToBoth() {
        // Given
        final EntityId elementId1 = new EntitySeed("identifier");

        // When
        final GetAdjacentIds op = new GetAdjacentIds.Builder()
                .input(elementId1)
                .directedType(DirectedType.BOTH)
                .build();

        // Then
        assertEquals(DirectedType.BOTH, op.getDirectedType());
    }

    private void shouldSerialiseAndDeserialiseOperationWithEntityIds() throws SerialisationException {
        // Given
        final EntityId entitySeed = new EntitySeed("identifier");
        final GetAdjacentIds op = new GetAdjacentIds.Builder()
                .input(entitySeed)
                .build();

        // When
        byte[] json = serialiser.serialise(op, true);
        final GetAdjacentIds deserialisedOp = serialiser.deserialise(json, GetAdjacentIds.class);

        // Then
        final Iterator itr = deserialisedOp.getInput().iterator();
        assertEquals(entitySeed, itr.next());
        assertFalse(itr.hasNext());
    }

    private void builderShouldCreatePopulatedOperationAll() {
        final GetAdjacentIds op = new GetAdjacentIds.Builder()
                .input(new EntitySeed("A"))
                .inOutType(IncludeIncomingOutgoingType.BOTH)
                .view(new View.Builder()
                        .edge("testEdgeGroup")
                        .build())
                .build();

        assertEquals(IncludeIncomingOutgoingType.BOTH, op.getIncludeIncomingOutGoing());
        assertNotNull(op.getView());
    }

    @Test
    public void shouldSetIncludeIncomingOutgoingTypeToBoth() {
        final EntityId elementId = new EntitySeed("identifier");

        // When
        final GetAdjacentIds op = new GetAdjacentIds.Builder()
                .input(elementId)
                .inOutType(IncludeIncomingOutgoingType.BOTH)
                .build();

        // Then
        assertEquals(IncludeIncomingOutgoingType.BOTH, op.getIncludeIncomingOutGoing());
    }

    @Test
    public void shouldSetOptionToValue() {
        // When
        final GetAdjacentIds op = new GetAdjacentIds.Builder()
                .option("key", "value")
                .build();

        // Then
        assertThat(op.getOptions(), is(notNullValue()));
        assertThat(op.getOptions().get("key"), is("value"));
    }

    private void builderShouldCreatePopulatedOperationIncoming() {
        EntityId seed = new EntitySeed("A");
        GetAdjacentIds op = new GetAdjacentIds.Builder()
                .input(seed)
                .inOutType(IncludeIncomingOutgoingType.INCOMING)
                .view(new View.Builder()
                        .edge("testEdgeGroup")
                        .build())
                .build();
        assertEquals(IncludeIncomingOutgoingType.INCOMING,
                op.getIncludeIncomingOutGoing());
        assertNotNull(op.getView());
        assertEquals(seed, op.getInput().iterator().next());
    }

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        shouldSerialiseAndDeserialiseOperationWithEntityIds();
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        builderShouldCreatePopulatedOperationAll();
        builderShouldCreatePopulatedOperationIncoming();
    }
}
