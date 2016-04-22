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

public class GetElementsBetweenSetsTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final EntitySeed seed1 = new EntitySeed("source1");
        final EntitySeed seed2 = new EntitySeed("destination1");
        final EntitySeed seed3 = new EntitySeed("source2");
        final EntitySeed seed4 = new EntitySeed("destination2");
        final GetElementsBetweenSets op = new GetElementsBetweenSets(Arrays.asList(seed1, seed2), Arrays.asList(seed3, seed4));

        // When
        byte[] json = serialiser.serialise(op, true);

        final GetElementsBetweenSets deserialisedOp = serialiser.deserialise(json, GetElementsBetweenSets.class);

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
        GetElementsBetweenSets getElementsBetweenSets = new GetElementsBetweenSets.Builder<>().addSeed(new EntitySeed("B")).addSeedB(new EntitySeed("A")).includeEdges(GetOperation.IncludeEdgeType.UNDIRECTED).includeEntities(true).inOutType(GetOperation.IncludeIncomingOutgoingType.INCOMING).option("testOption", "true").populateProperties(false).summarise(false).view(new View.Builder().edge("testEdgeGroup").build()).build();
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
