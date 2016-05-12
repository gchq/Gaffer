package gaffer.accumulostore.operation.impl;

import gaffer.accumulostore.retriever.impl.data.AccumuloRetrieverTestData;
import gaffer.data.elementdefinition.view.View;
import gaffer.exception.SerialisationException;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.operation.GetOperation;
import gaffer.operation.OperationTest;
import gaffer.operation.data.EntitySeed;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.*;

public class GetElementsBetweenSetsTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final GetElementsBetweenSets op = new GetElementsBetweenSets(
                Arrays.asList(AccumuloRetrieverTestData.SEED_SOURCE_1, AccumuloRetrieverTestData.SEED_DESTINATION_1),
                Arrays.asList(AccumuloRetrieverTestData.SEED_SOURCE_2, AccumuloRetrieverTestData.SEED_DESTINATION_2));

        // When
        byte[] json = serialiser.serialise(op, true);

        final GetElementsBetweenSets deserialisedOp = serialiser.deserialise(json, GetElementsBetweenSets.class);

        // Then
        final Iterator itrSeedsA = deserialisedOp.getSeeds().iterator();
        assertEquals(AccumuloRetrieverTestData.SEED_SOURCE_1, itrSeedsA.next());
        assertEquals(AccumuloRetrieverTestData.SEED_DESTINATION_1, itrSeedsA.next());
        assertFalse(itrSeedsA.hasNext());

        final Iterator itrSeedsB = deserialisedOp.getSeedsB().iterator();
        assertEquals(AccumuloRetrieverTestData.SEED_SOURCE_2, itrSeedsB.next());
        assertEquals(AccumuloRetrieverTestData.SEED_DESTINATION_2, itrSeedsB.next());
        assertFalse(itrSeedsB.hasNext());

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final GetElementsBetweenSets getElementsBetweenSets = new GetElementsBetweenSets.Builder<>().addSeed(AccumuloRetrieverTestData.SEED_B)
                .addSeedB(AccumuloRetrieverTestData.SEED_A).includeEdges(GetOperation.IncludeEdgeType.UNDIRECTED)
                .includeEntities(true).inOutType(GetOperation.IncludeIncomingOutgoingType.INCOMING)
                .option("testOption", "true").populateProperties(false)
                .summarise(false).view(new View.Builder().edge("testEdgeGroup").build()).build();
        assertEquals("true", getElementsBetweenSets.getOption("testOption"));
        assertTrue(getElementsBetweenSets.isIncludeEntities());
        assertEquals(GetOperation.IncludeEdgeType.UNDIRECTED, getElementsBetweenSets.getIncludeEdges());
        assertEquals(GetOperation.IncludeIncomingOutgoingType.INCOMING, getElementsBetweenSets.getIncludeIncomingOutGoing());
        assertFalse(getElementsBetweenSets.isPopulateProperties());
        assertFalse(getElementsBetweenSets.isSummarise());
        assertEquals(new EntitySeed("B"), getElementsBetweenSets.getInput().iterator().next());
        assertEquals(new EntitySeed("A"), getElementsBetweenSets.getSeedsB().iterator().next());
        assertNotNull(getElementsBetweenSets.getView());
    }
}
