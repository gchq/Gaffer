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
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.GetOperation.SeedMatchingType;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class GetElementsTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    public void shouldSetSeedMatchingTypeToEquals() {
        // Given
        final ElementSeed elementSeed1 = new EntitySeed("identifier");

        // When
        final GetElements op = new GetElements.Builder<>().seeds(Collections.singletonList(elementSeed1))
                                                          .seedMatching(SeedMatchingType.EQUAL)
                                                          .build();

        // Then
        assertEquals(GetOperation.SeedMatchingType.EQUAL, op.getSeedMatching());
    }

    private void shouldSerialiseAndDeserialiseOperationWithElementSeeds() throws SerialisationException {
        // Given
        final ElementSeed elementSeed1 = new EntitySeed("identifier");
        final ElementSeed elementSeed2 = new EdgeSeed("source2", "destination2", true);
        final GetElements op = new GetElements(Arrays.asList(elementSeed1, elementSeed2));

        // When
        byte[] json = serialiser.serialise(op, true);
        final GetElements deserialisedOp = serialiser.deserialise(json, GetElements.class);

        // Then
        final Iterator itr = deserialisedOp.getSeeds().iterator();
        assertEquals(elementSeed1, itr.next());
        assertEquals(elementSeed2, itr.next());
        assertFalse(itr.hasNext());
    }

    private void builderShouldCreatePopulatedOperationAll() {
        final GetElements op = new GetElements.Builder<>()
                .addSeed(new EntitySeed("A"))
                .includeEdges(GetOperation.IncludeEdgeType.ALL)
                .includeEntities(false)
                .inOutType(GetOperation.IncludeIncomingOutgoingType.BOTH)
                .option("testOption", "true")
                .populateProperties(false)
                .view(new View.Builder()
                        .edge("testEdgeGroup")
                        .build())
                .build();

        assertFalse(op.isIncludeEntities());
        assertFalse(op.isPopulateProperties());
        assertEquals(GetOperation.IncludeIncomingOutgoingType.BOTH,
                op.getIncludeIncomingOutGoing());
        assertEquals(GetOperation.IncludeEdgeType.ALL, op.getIncludeEdges());
        assertEquals("true", op.getOption("testOption"));
        assertNotNull(op.getView());
    }

    @Test
    public void shouldSetSeedMatchingTypeToRelated() {
        final ElementSeed elementSeed1 = new EntitySeed("identifier");
        final ElementSeed elementSeed2 = new EdgeSeed("source2", "destination2", true);

        // When
        final GetElements op = new GetElements(Arrays.asList(elementSeed1, elementSeed2));

        // Then
        assertEquals(GetOperation.SeedMatchingType.RELATED, op.getSeedMatching());
    }

    private void builderShouldCreatePopulatedOperationIncoming() {
        ElementSeed seed = new EntitySeed("A");
        GetElements op = new GetElements.Builder<>()
                .addSeed(seed)
                .includeEdges(GetOperation.IncludeEdgeType.UNDIRECTED)
                .includeEntities(false)
                .inOutType(GetOperation.IncludeIncomingOutgoingType.INCOMING)
                .option("testOption", "true").populateProperties(false)
                .view(new View.Builder()
                        .edge("testEdgeGroup")
                        .build())
                .build();
        assertEquals("true", op.getOption("testOption"));
        assertFalse(op.isPopulateProperties());
        assertFalse(op.isIncludeEntities());
        assertEquals(GetOperation.IncludeIncomingOutgoingType.INCOMING, op
                .getIncludeIncomingOutGoing());
        assertEquals(GetOperation.IncludeEdgeType.UNDIRECTED, op
                .getIncludeEdges());
        assertNotNull(op.getView());
        assertEquals(seed, op.getSeeds().iterator().next());
    }

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        shouldSerialiseAndDeserialiseOperationWithElementSeeds();
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        builderShouldCreatePopulatedOperationAll();
        builderShouldCreatePopulatedOperationIncoming();
    }
}
