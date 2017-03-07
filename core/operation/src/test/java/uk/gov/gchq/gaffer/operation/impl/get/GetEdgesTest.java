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

import junit.framework.TestCase;
import org.junit.Test;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.SeedMatching.SeedMatchingType;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import java.util.Collections;
import java.util.Iterator;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class GetEdgesTest implements OperationTest {

    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        shouldSerialiseAndDeserialiseOperationWithEdgeId();
        shouldSerialiseAndDeserialiseOperationWithEntityId();
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        builderShouldCreatePopulatedOperationWithEdgeId();
        builderShouldCreatePopulatedOperationWithEntityId();
    }

    @Test
    public void shouldSetSeedMatchingTypeToRelated() {
        // Given
        final EntityId seed1 = new EntitySeed("identifier1");

        // When
        final GetEdges op = new GetEdges.Builder<>()
                .addSeed(seed1)
                .build();

        // Then
        assertEquals(SeedMatching.SeedMatchingType.RELATED, op.getSeedMatching());
    }

    @Test
    public void shouldSetSeedMatchingTypeToEquals() {
        // Given
        final EdgeId seed1 = new EdgeSeed("source1", "destination1", true);

        // When
        final GetEdges op = new GetEdges.Builder<EdgeId>().seeds(Collections.singletonList(seed1))
                .seedMatching(SeedMatchingType.EQUAL)
                .build();

        // Then
        assertEquals(SeedMatching.SeedMatchingType.EQUAL, op.getSeedMatching());
    }

    private void shouldSerialiseAndDeserialiseOperationWithEdgeId() throws SerialisationException {
        // Given
        final EdgeId seed1 = new EdgeSeed("source1", "destination1", true);
        final EdgeId seed2 = new EdgeSeed("source2", "destination2", true);
        final GetEdges op = new GetEdges.Builder<>()
                .addSeed(seed1)
                .addSeed(seed2)
                .build();

        // When
        byte[] json = serialiser.serialise(op, true);
        final GetEdges deserialisedOp = serialiser.deserialise(json, GetEdges.class);

        // Then
        final Iterator itr = deserialisedOp.getSeeds().iterator();
        assertEquals(seed1, itr.next());
        assertEquals(seed2, itr.next());
        assertFalse(itr.hasNext());
    }

    private void builderShouldCreatePopulatedOperationWithEdgeId() {
        EdgeId seed = new EdgeSeed("A", "B", true);
        GetEdges op = new GetEdges.Builder<>()
                .addSeed(seed)
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .option("testOption", "true")
                .view(new View.Builder()
                        .edge("testEdgeGroup")
                        .build())
                .build();
        assertNotNull(op.getView());
        assertEquals("true", op.getOption("testOption"));
        assertEquals(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING, op
                .getIncludeIncomingOutGoing());
        assertEquals(seed, op.getInput().iterator().next());
    }

    private void shouldSerialiseAndDeserialiseOperationWithEntityId() throws SerialisationException {
        // Given
        final EntityId seed1 = new EntitySeed("identifier1");
        final EntityId seed2 = new EntitySeed("identifier2");
        final GetEdges op = new GetEdges.Builder<>()
                .addSeed(seed1)
                .addSeed(seed2)
                .build();

        // When
        byte[] json = serialiser.serialise(op, true);
        final GetEdges deserialisedOp = serialiser.deserialise(json, GetEdges.class);

        // Then
        final Iterator itr = deserialisedOp.getSeeds().iterator();
        assertEquals(seed1, itr.next());
        assertEquals(seed2, itr.next());
        TestCase.assertFalse(itr.hasNext());
    }

    private void builderShouldCreatePopulatedOperationWithEntityId() {
        final GetEdges op = new GetEdges.Builder<>()
                .addSeed(new EntitySeed("A"))
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .option("testOption", "true")
                .view(new View.Builder()
                        .edge("testEdgeGroup")
                        .build())
                .build();
        assertEquals(SeededGraphFilters.IncludeIncomingOutgoingType.OUTGOING, op
                .getIncludeIncomingOutGoing());
        assertEquals("true", op.getOption("testOption"));
        assertNotNull(op.getView());
    }
}
