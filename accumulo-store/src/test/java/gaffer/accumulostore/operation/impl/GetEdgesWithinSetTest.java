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

public class GetEdgesWithinSetTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    public void shouldNotReturnEntities(){

        final GetEdgesWithinSet op = new GetEdgesWithinSet();
        assertFalse(op.isIncludeEntities());

    }

    @Test
    public void shouldNotBeAbleToSetNoEdges(){

        final GetEdgesWithinSet op = new GetEdgesWithinSet();

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
        final GetEdgesWithinSet op = new GetEdgesWithinSet(
                Arrays.asList(AccumuloRetrieverTestData.SEED_SOURCE_1, AccumuloRetrieverTestData.SEED_DESTINATION_1,
                        AccumuloRetrieverTestData.SEED_SOURCE_2, AccumuloRetrieverTestData.SEED_DESTINATION_2));

        // When
        byte[] json = serialiser.serialise(op, true);

        final GetEdgesWithinSet deserialisedOp = serialiser.deserialise(json, GetEdgesWithinSet.class);

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
        final GetEdgesWithinSet getEdgesWithinSet = new  GetEdgesWithinSet.Builder()
                .includeEdges(GetOperation.IncludeEdgeType.DIRECTED)
                .addSeed(AccumuloRetrieverTestData.SEED_A).option("testOption", "true").populateProperties(false)
                .summarise(true).view(new View.Builder().edge("testEdgeGroup").build()).build();
        assertTrue(getEdgesWithinSet.isSummarise());
        assertFalse(getEdgesWithinSet.isPopulateProperties());
        assertEquals(GetOperation.IncludeEdgeType.DIRECTED, getEdgesWithinSet.getIncludeEdges());
        assertEquals("true", getEdgesWithinSet.getOption("testOption"));
        assertEquals(new EntitySeed("A"), getEdgesWithinSet.getSeeds().iterator().next());
        assertNotNull(getEdgesWithinSet.getView());
    }
}
