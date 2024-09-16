/*
 * Copyright 2016-2023 Crown Copyright
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

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(op.getDirectedType()).isEqualTo(DirectedType.EITHER);
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertThat(outputClass).isEqualTo(Iterable.class);
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
        assertThat(itr.next()).isEqualTo(entitySeed);
        assertThat(itr).isExhausted();
    }

    private void builderShouldCreatePopulatedOperationAll() {
        final GetAdjacentIds op = new GetAdjacentIds.Builder()
                .input(new EntitySeed("A"))
                .inOutType(IncludeIncomingOutgoingType.EITHER)
                .view(new View.Builder()
                        .edge("testEdgeGroup")
                        .build())
                .build();

        assertThat(op.getIncludeIncomingOutGoing()).isEqualTo(IncludeIncomingOutgoingType.EITHER);
        assertThat(op.getView()).isNotNull();
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
        assertThat(op.getIncludeIncomingOutGoing()).isEqualTo(IncludeIncomingOutgoingType.EITHER);
    }

    @Test
    public void shouldSetOptionToValue() {
        // When
        final GetAdjacentIds op = new GetAdjacentIds.Builder()
                .option("key", "value")
                .build();

        // Then
        assertThat(op.getOptions()).isNotNull()
                .containsEntry("key", "value");
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
        assertThat(op.getIncludeIncomingOutGoing()).isEqualTo(IncludeIncomingOutgoingType.INCOMING);
        assertThat(op.getView()).isNotNull();
        assertThat(op.getInput().iterator().next()).isEqualTo(seed);
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        builderShouldCreatePopulatedOperationAll();
        builderShouldCreatePopulatedOperationIncoming();
    }

    @Test
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
        assertThat(clone).isNotSameAs(getAdjacentIds);
        assertThat(clone.getDirectedType()).isEqualTo(DirectedType.DIRECTED);
        assertThat(clone.getIncludeIncomingOutGoing()).isEqualTo(IncludeIncomingOutgoingType.INCOMING);
        assertThat(view).isEqualTo(clone.getView());
        assertThat(clone.getInput().iterator().next()).isEqualTo(input);
    }

    @Test
    public void shouldSetInputFromVerticesAndEntityIds() {
        // When
        final GetAdjacentIds op = new GetAdjacentIds.Builder()
                .input("1", new EntitySeed("2"), new Entity("group1", "3"))
                .build();

        // Then
        assertThat(Lists.newArrayList(op.getInput()))
                .isEqualTo(Lists.newArrayList(new EntitySeed("1"), new EntitySeed("2"), new Entity("group1", "3"))
        );
    }

    @Override
    protected GetAdjacentIds getTestObject() {
        return new GetAdjacentIds();
    }
}
