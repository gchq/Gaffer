package uk.gov.gchq.gaffer.accumulostore.operation.impl;

import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloTestData;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.OperationTest;
import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GetElementsBetweenSetsTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final GetElementsBetweenSets op = new GetElementsBetweenSets(
                Arrays.asList(AccumuloTestData.SEED_SOURCE_1, AccumuloTestData.SEED_DESTINATION_1),
                Arrays.asList(AccumuloTestData.SEED_SOURCE_2, AccumuloTestData.SEED_DESTINATION_2));

        // When
        byte[] json = serialiser.serialise(op, true);

        final GetElementsBetweenSets deserialisedOp = serialiser.deserialise(json, GetElementsBetweenSets.class);

        // Then
        final Iterator itrSeedsA = deserialisedOp.getSeeds().iterator();
        assertEquals(AccumuloTestData.SEED_SOURCE_1, itrSeedsA.next());
        assertEquals(AccumuloTestData.SEED_DESTINATION_1, itrSeedsA.next());
        assertFalse(itrSeedsA.hasNext());

        final Iterator itrSeedsB = deserialisedOp.getSeedsB().iterator();
        assertEquals(AccumuloTestData.SEED_SOURCE_2, itrSeedsB.next());
        assertEquals(AccumuloTestData.SEED_DESTINATION_2, itrSeedsB.next());
        assertFalse(itrSeedsB.hasNext());

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final GetElementsBetweenSets getElementsBetweenSets = new GetElementsBetweenSets.Builder<>().addSeed(AccumuloTestData.SEED_B)
                .addSeedB(AccumuloTestData.SEED_A).includeEdges(GetOperation.IncludeEdgeType.UNDIRECTED)
                .includeEntities(true).inOutType(GetOperation.IncludeIncomingOutgoingType.INCOMING)
                .option(AccumuloTestData.TEST_OPTION_PROPERTY_KEY, "true").populateProperties(false)
                .view(new View.Builder().edge("testEdgeGroup").build()).build();
        assertEquals("true", getElementsBetweenSets.getOption(AccumuloTestData.TEST_OPTION_PROPERTY_KEY));
        assertTrue(getElementsBetweenSets.isIncludeEntities());
        assertEquals(GetOperation.IncludeEdgeType.UNDIRECTED, getElementsBetweenSets.getIncludeEdges());
        assertEquals(GetOperation.IncludeIncomingOutgoingType.INCOMING, getElementsBetweenSets.getIncludeIncomingOutGoing());
        assertFalse(getElementsBetweenSets.isPopulateProperties());
        assertEquals(AccumuloTestData.SEED_B, getElementsBetweenSets.getInput().iterator().next());
        assertEquals(AccumuloTestData.SEED_A, getElementsBetweenSets.getSeedsB().iterator().next());
        assertNotNull(getElementsBetweenSets.getView());
    }
}
