package uk.gov.gchq.gaffer.accumulostore.operation.impl;


import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloTestData;
import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class GetEntitiesInRangesTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    public void shouldNotReturnEdges() {

        final GetEntitiesInRanges<Pair<EntitySeed>> op = new GetEntitiesInRanges<>();
        assertEquals(GetOperation.IncludeEdgeType.NONE, op.getIncludeEdges());

    }

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final List<Pair<EntitySeed>> pairList = new ArrayList<>();
        final Pair<EntitySeed> pair1 = new Pair<>(AccumuloTestData.SEED_SOURCE_1, AccumuloTestData.SEED_DESTINATION_1);
        final Pair<EntitySeed> pair2 = new Pair<>(AccumuloTestData.SEED_SOURCE_2, AccumuloTestData.SEED_DESTINATION_2);
        pairList.add(pair1);
        pairList.add(pair2);
        final GetEntitiesInRanges<Pair<EntitySeed>> op = new GetEntitiesInRanges<>(pairList);
        // When
        byte[] json = serialiser.serialise(op, true);

        final GetEntitiesInRanges<Pair<EntitySeed>> deserialisedOp = serialiser.deserialise(json, GetEntitiesInRanges.class);

        // Then
        final Iterator itrPairs = deserialisedOp.getSeeds().iterator();
        assertEquals(pair1, itrPairs.next());
        assertEquals(pair2, itrPairs.next());
        assertFalse(itrPairs.hasNext());

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final Pair<EntitySeed> seed = new Pair<>(AccumuloTestData.SEED_A, AccumuloTestData.SEED_B);
        final GetEntitiesInRanges getEntitiesInRanges = new GetEntitiesInRanges.Builder<Pair<EntitySeed>>()
                .option(AccumuloTestData.TEST_OPTION_PROPERTY_KEY, "true")
                .populateProperties(false)
                .view(new View.Builder().edge("testEdgeGroup").build())
                .addSeed(seed).build();
        assertEquals(seed, getEntitiesInRanges.getSeeds().iterator().next());
        assertEquals("true", getEntitiesInRanges.getOption(AccumuloTestData.TEST_OPTION_PROPERTY_KEY));
        assertFalse(getEntitiesInRanges.isPopulateProperties());
        assertNotNull(getEntitiesInRanges.getView());
    }
}
