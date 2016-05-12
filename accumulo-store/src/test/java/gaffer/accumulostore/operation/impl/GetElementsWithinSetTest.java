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

public class GetElementsWithinSetTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final GetElementsWithinSet op = new GetElementsWithinSet(Arrays.asList(AccumuloRetrieverTestData.SEED_SOURCE_1,
                AccumuloRetrieverTestData.SEED_DESTINATION_1, AccumuloRetrieverTestData.SEED_SOURCE_2, AccumuloRetrieverTestData.SEED_DESTINATION_2));

        // When
        byte[] json = serialiser.serialise(op, true);

        final GetElementsWithinSet deserialisedOp = serialiser.deserialise(json, GetElementsWithinSet.class);

        // Then
        final Iterator itrSeedsA = deserialisedOp.getSeeds().iterator();
        assertEquals(AccumuloRetrieverTestData.SEED_SOURCE_1, itrSeedsA.next());
        assertEquals(AccumuloRetrieverTestData.SEED_DESTINATION_1, itrSeedsA.next());
        assertEquals(AccumuloRetrieverTestData.SEED_SOURCE_2, itrSeedsA.next());
        assertEquals(AccumuloRetrieverTestData.SEED_DESTINATION_2, itrSeedsA.next());
        assertFalse(itrSeedsA.hasNext());

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final GetElementsWithinSet getElementsWithinSet = new GetElementsWithinSet.Builder<>().addSeed(AccumuloRetrieverTestData.SEED_A)
                .includeEdges(GetOperation.IncludeEdgeType.NONE).includeEntities(true).option("testOption", "true")
                .populateProperties(true).summarise(false).view(new View.Builder().edge("testEdgegroup").build()).build();
        assertEquals("true", getElementsWithinSet.getOption("testOption"));
        assertTrue(getElementsWithinSet.isIncludeEntities());
        assertEquals(GetOperation.IncludeEdgeType.NONE, getElementsWithinSet.getIncludeEdges());
        assertTrue(getElementsWithinSet.isPopulateProperties());
        assertFalse(getElementsWithinSet.isSummarise());
        assertEquals(new EntitySeed("A"), getElementsWithinSet.getInput().iterator().next());
        assertNotNull(getElementsWithinSet.getView());
    }
}
