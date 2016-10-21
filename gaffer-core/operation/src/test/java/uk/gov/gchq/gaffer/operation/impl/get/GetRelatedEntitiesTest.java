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

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import org.junit.Test;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;


public class GetRelatedEntitiesTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    public void shouldSetSeedMatchingTypeToRelated() {
        // Given
        final EdgeSeed seed1 = new EdgeSeed("source1", "destination1", true);

        // When
        final GetRelatedEntities<EdgeSeed> op = new GetRelatedEntities<>(Collections.singletonList(seed1));

        // Then
        assertEquals(GetOperation.SeedMatchingType.RELATED, op.getSeedMatching());
    }

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final EdgeSeed seed1 = new EdgeSeed("source1", "destination1", true);
        final EdgeSeed seed2 = new EdgeSeed("source2", "destination2", false);
        final GetRelatedEntities<EdgeSeed> op = new GetRelatedEntities<>(Arrays.asList(seed1, seed2));

        // When
        byte[] json = serialiser.serialise(op, true);
        final GetRelatedEntities deserialisedOp = serialiser.deserialise(json, GetRelatedEntities.class);

        // Then
        final Iterator itr = deserialisedOp.getSeeds().iterator();
        assertEquals(seed1, itr.next());
        assertEquals(seed2, itr.next());
        assertFalse(itr.hasNext());
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final EntitySeed seed = new EntitySeed("A");

        // When
        final GetRelatedEntities<EntitySeed> getRelatedElements = new GetRelatedEntities.Builder<EntitySeed>()
                .addSeed(seed)
                .option("testOption", "true")
                .populateProperties(false)
                .view(new View.Builder()
                        .edge(TestGroups.ENTITY)
                        .build())
                .build();

        // Then
        assertEquals("true", getRelatedElements.getOption("testOption"));
        assertFalse(getRelatedElements.isPopulateProperties());
        assertNotNull(getRelatedElements.getView());
        assertEquals(seed, getRelatedElements.getSeeds().iterator().next());
    }
}
