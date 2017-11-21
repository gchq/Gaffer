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

import com.google.common.collect.Lists;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;

import java.util.Iterator;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;

public class GetAdjacentIdsTest extends OperationTest<GetAdjacentIds> {
    @Test
    public void shouldSetDirectedTypeToBoth() {
        // Given
        final EntityId elementId1 = new EntitySeed("identifier");

        // When
        final GetAdjacentIds op = new GetAdjacentIds.Builder()
                .input(elementId1)
                .directedType(DirectedType.EITHER)
                .build();

        // Then
        assertEquals(DirectedType.EITHER, op.getDirectedType());
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertEquals(CloseableIterable.class, outputClass);
    }

    @Test
    public void shouldSerialiseAndDeserialiseOperationWithEntityIds() throws SerialisationException {
        // Given
        final EntityId entitySeed = new EntitySeed("identifier");
        final GetAdjacentIds op = new GetAdjacentIds.Builder()
                .input(entitySeed)
                .build();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);
        final GetAdjacentIds deserialisedOp = JSONSerialiser.deserialise(json, GetAdjacentIds.class);

        // Then
        final Iterator itr = deserialisedOp.getInput().iterator();
        assertEquals(entitySeed, itr.next());
        assertFalse(itr.hasNext());
    }

    private void builderShouldCreatePopulatedOperationAll() {
        final GetAdjacentIds op = new GetAdjacentIds.Builder()
                .input(new EntitySeed("A"))
                .inOutType(IncludeIncomingOutgoingType.EITHER)
                .view(new View.Builder()
                        .edge("testEdgeGroup")
                        .build())
                .build();

        assertEquals(IncludeIncomingOutgoingType.EITHER, op.getIncludeIncomingOutGoing());
        assertNotNull(op.getView());
    }

    @Test
    public void shouldSetIncludeIncomingOutgoingTypeToBoth() {
        final EntityId elementId = new EntitySeed("identifier");

        // When
        final GetAdjacentIds op = new GetAdjacentIds.Builder()
                .input(elementId)
                .inOutType(IncludeIncomingOutgoingType.EITHER)
                .build();

        // Then
        assertEquals(IncludeIncomingOutgoingType.EITHER, op.getIncludeIncomingOutGoing());
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
    public void builderShouldCreatePopulatedOperation() {
        builderShouldCreatePopulatedOperationAll();
        builderShouldCreatePopulatedOperationIncoming();
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        EntityId input = new EntitySeed("A");
        View view = new View.Builder()
                .edge("testEdgeGroup")
                .build();
        GetAdjacentIds getAdjacentIds = new GetAdjacentIds.Builder()
                .input(input)
                .inOutType(IncludeIncomingOutgoingType.INCOMING)
                .directedType(DirectedType.DIRECTED)
                .view(view)
                .build();

        // When
        GetAdjacentIds clone = getAdjacentIds.shallowClone();


        // Then
        assertNotSame(getAdjacentIds, clone);
        assertEquals(DirectedType.DIRECTED, clone.getDirectedType());
        assertEquals(IncludeIncomingOutgoingType.INCOMING,
                clone.getIncludeIncomingOutGoing());
        assertEquals(view, clone.getView());
        assertEquals(Lists.newArrayList(input), clone.getInput());
    }

    @Override
    protected GetAdjacentIds getTestObject() {
        return new GetAdjacentIds();
    }
}
