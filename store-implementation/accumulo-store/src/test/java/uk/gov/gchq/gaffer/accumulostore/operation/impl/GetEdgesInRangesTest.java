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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GetEdgesInRangesTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    public void shouldNotReturnEntities() {

        final GetEdgesInRanges<Pair<EntitySeed>> op = new GetEdgesInRanges<>();
        assertFalse(op.isIncludeEntities());

    }

    @Test
    public void shouldNotBeAbleToSetNoEdges() {

        final GetEdgesInRanges<Pair<EntitySeed>> op = new GetEdgesInRanges<>();

        try {
            op.setIncludeEdges(GetOperation.IncludeEdgeType.NONE);
        } catch (final IllegalArgumentException e) {
            assertTrue(true);
            return;
        }
        fail();

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
        final GetEdgesInRanges<Pair<EntitySeed>> op = new GetEdgesInRanges<>(pairList);
        // When
        byte[] json = serialiser.serialise(op, true);

        final GetEdgesInRanges<Pair<EntitySeed>> deserialisedOp = serialiser.deserialise(json, GetEdgesInRanges.class);

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
        final GetEdgesInRanges getEdgesInRanges = new GetEdgesInRanges.Builder<>()
                .includeEdges(GetOperation.IncludeEdgeType.DIRECTED).inOutType(GetOperation.IncludeIncomingOutgoingType.BOTH)
                .addSeed(seed).option(AccumuloTestData.TEST_OPTION_PROPERTY_KEY, "true").populateProperties(false)
                .view(new View.Builder().edge("testEdgeGroup").build()).build();
        assertFalse(getEdgesInRanges.isPopulateProperties());
        assertEquals(GetOperation.IncludeEdgeType.DIRECTED, getEdgesInRanges.getIncludeEdges());
        assertEquals(GetOperation.IncludeIncomingOutgoingType.BOTH, getEdgesInRanges.getIncludeIncomingOutGoing());
        assertEquals("true", getEdgesInRanges.getOption(AccumuloTestData.TEST_OPTION_PROPERTY_KEY));
        assertEquals(seed, getEdgesInRanges.getSeeds().iterator().next());
        assertNotNull(getEdgesInRanges.getView());
    }
}

