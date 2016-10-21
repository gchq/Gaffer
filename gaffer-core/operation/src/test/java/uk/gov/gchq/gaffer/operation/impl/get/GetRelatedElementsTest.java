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

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import org.junit.Test;
import java.util.Arrays;
import java.util.Iterator;


public class GetRelatedElementsTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    public void shouldSetSeedMatchingTypeToRelated() {
        final ElementSeed elementSeed1 = new EntitySeed("identifier");
        final ElementSeed elementSeed2 = new EdgeSeed("source2", "destination2", true);

        // When
        final GetRelatedElements op = new GetRelatedElements(Arrays.asList(elementSeed1, elementSeed2));

        // Then
        assertEquals(GetOperation.SeedMatchingType.RELATED, op.getSeedMatching());
    }

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final ElementSeed elementSeed1 = new EntitySeed("identifier");
        final ElementSeed elementSeed2 = new EdgeSeed("source2", "destination2", true);
        final GetRelatedElements op = new GetRelatedElements(Arrays.asList(elementSeed1, elementSeed2));

        // When
        byte[] json = serialiser.serialise(op, true);
        final GetRelatedElements deserialisedOp = serialiser.deserialise(json, GetRelatedElements.class);

        // Then
        final Iterator itr = deserialisedOp.getSeeds().iterator();
        assertEquals(elementSeed1, itr.next());
        assertEquals(elementSeed2, itr.next());
        assertFalse(itr.hasNext());
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        ElementSeed seed = new EntitySeed("A");
        GetRelatedElements getRelatedElements = new GetRelatedElements.Builder<>()
                .addSeed(seed)
                .includeEdges(GetOperation.IncludeEdgeType.UNDIRECTED)
                .includeEntities(false)
                .inOutType(GetOperation.IncludeIncomingOutgoingType.INCOMING)
                .option("testOption", "true").populateProperties(false)
                .view(new View.Builder()
                        .edge("testEdgeGroup")
                        .build())
                .build();
        assertEquals("true", getRelatedElements.getOption("testOption"));
        assertFalse(getRelatedElements.isPopulateProperties());
        assertFalse(getRelatedElements.isIncludeEntities());
        assertEquals(GetOperation.IncludeIncomingOutgoingType.INCOMING, getRelatedElements.getIncludeIncomingOutGoing());
        assertEquals(GetOperation.IncludeEdgeType.UNDIRECTED, getRelatedElements.getIncludeEdges());
        assertNotNull(getRelatedElements.getView());
        assertEquals(seed, getRelatedElements.getSeeds().iterator().next());
    }
}
