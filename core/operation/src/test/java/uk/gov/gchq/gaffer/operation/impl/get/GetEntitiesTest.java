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
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.GetOperation.SeedMatchingType;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class GetEntitiesTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        shouldSerialiseAndDeserialiseOperationWithEdgeSeed();
        shouldSerialiseAndDeserialiseOperationWithEntitySeed();
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        builderShouldCreatePopulatedOperationWithEntitySeed();
    }

    @Test
    public void shouldSetSeedMatchingTypeToEquals() {
        // Given
        final EntitySeed seed1 = new EntitySeed("identifier");

        // When
        final GetEntities op = new GetEntities.Builder<EntitySeed>().seeds(Collections.singletonList(seed1))
                .seedMatching(SeedMatchingType.EQUAL)
                .build();

        // Then
        assertEquals(GetOperation.SeedMatchingType.EQUAL, op.getSeedMatching());
    }

    private void shouldSerialiseAndDeserialiseOperationWithEntitySeed() throws SerialisationException {
        // Given
        final EntitySeed seed1 = new EntitySeed("id1");
        final EntitySeed seed2 = new EntitySeed("id2");
        final GetEntities op = new GetEntities.Builder<EntitySeed>().seeds(Arrays.asList(seed1, seed2))
                .seedMatching(SeedMatchingType.EQUAL)
                .build();

        // When
        byte[] json = serialiser.serialise(op, true);
        final GetEntities deserialisedOp = serialiser.deserialise(json, GetEntities.class);

        // Then
        final Iterator itr = deserialisedOp.getSeeds().iterator();
        assertEquals(seed1, itr.next());
        assertEquals(seed2, itr.next());
        assertFalse(itr.hasNext());
    }

    private void shouldSetSeedMatchingTypeToRelatedWithEdgeSeed() {
        // Given
        final EdgeSeed seed1 = new EdgeSeed("source1", "destination1", true);

        // When
        final GetEntities op = new GetEntities(Collections.singletonList(seed1));

        // Then
        assertEquals(GetOperation.SeedMatchingType.RELATED, op.getSeedMatching());
    }

    private void shouldSerialiseAndDeserialiseOperationWithEdgeSeed() throws SerialisationException {
        // Given
        final EdgeSeed seed1 = new EdgeSeed("source1", "destination1", true);
        final EdgeSeed seed2 = new EdgeSeed("source2", "destination2", false);
        final GetEntities op = new GetEntities(Arrays.asList(seed1, seed2));

        // When
        byte[] json = serialiser.serialise(op, true);
        final GetEntities deserialisedOp = serialiser.deserialise(json, GetEntities.class);

        // Then
        final Iterator itr = deserialisedOp.getSeeds().iterator();
        assertEquals(seed1, itr.next());
        assertEquals(seed2, itr.next());
        assertFalse(itr.hasNext());
    }

    private void builderShouldCreatePopulatedOperationWithEntitySeed() {
        // Given
        final EntitySeed seed = new EntitySeed("A");

        // When
        final GetEntities op = new GetEntities.Builder<>()
                .addSeed(seed)
                .option("testOption", "true")
                .populateProperties(false)
                .view(new View.Builder()
                        .edge(TestGroups.ENTITY)
                        .build())
                .build();

        // Then
        assertEquals("true", op.getOption("testOption"));
        assertFalse(op.isPopulateProperties());
        assertNotNull(op.getView());
        assertEquals(seed, op.getSeeds().iterator().next());
    }
}
