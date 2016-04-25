package gaffer.accumulostore.operation.impl;


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

public class GetEdgesBetweenSetsTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    public void shouldNotReturnEntities(){

        final GetEdgesBetweenSets op = new GetEdgesBetweenSets();
        assertFalse(op.isIncludeEntities());

    }

    @Test
    public void shouldNotBeAbleToSetNoEdges(){

        final GetEdgesBetweenSets op = new GetEdgesBetweenSets();

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
        final EntitySeed seed1 = new EntitySeed("source1");
        final EntitySeed seed2 = new EntitySeed("destination1");
        final EntitySeed seed3 = new EntitySeed("source2");
        final EntitySeed seed4 = new EntitySeed("destination2");
        final GetEdgesBetweenSets op = new GetEdgesBetweenSets(Arrays.asList(seed1, seed2), Arrays.asList(seed3, seed4));

        // When
        byte[] json = serialiser.serialise(op, true);

        final GetEdgesBetweenSets deserialisedOp = serialiser.deserialise(json, GetEdgesBetweenSets.class);

        // Then
        final Iterator itrSeedsA = deserialisedOp.getSeeds().iterator();
        assertEquals(seed1, itrSeedsA.next());
        assertEquals(seed2, itrSeedsA.next());
        assertFalse(itrSeedsA.hasNext());

        final Iterator itrSeedsB = deserialisedOp.getSeedsB().iterator();
        assertEquals(seed3, itrSeedsB.next());
        assertEquals(seed4, itrSeedsB.next());
        assertFalse(itrSeedsB.hasNext());

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        GetEdgesBetweenSets getEdgesBetweenSets = new GetEdgesBetweenSets.Builder().includeEdges(GetOperation.IncludeEdgeType.ALL).addSeed(new EntitySeed("A")).addSeedB(new EntitySeed("B")).inOutType(GetOperation.IncludeIncomingOutgoingType.OUTGOING).option("testOption", "true").populateProperties(false).summarise(true).view(new View.Builder().edge("testEdgeGroup").build()).build();
        assertTrue(getEdgesBetweenSets.isSummarise());
        assertFalse(getEdgesBetweenSets.isPopulateProperties());
        assertEquals(GetOperation.IncludeEdgeType.ALL, getEdgesBetweenSets.getIncludeEdges());
        assertEquals(GetOperation.IncludeIncomingOutgoingType.OUTGOING, getEdgesBetweenSets.getIncludeIncomingOutGoing());
        assertEquals("true", getEdgesBetweenSets.getOption("testOption"));
        assertEquals(new EntitySeed("A"), getEdgesBetweenSets.getSeeds().iterator().next());
        assertEquals(new EntitySeed("B"), getEdgesBetweenSets.getSeedsB().iterator().next());
        assertNotNull(getEdgesBetweenSets.getView());
    }
}
