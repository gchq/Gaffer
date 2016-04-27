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

public class GetElementsWithinSetTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final EntitySeed seed1 = new EntitySeed("source1");
        final EntitySeed seed2 = new EntitySeed("destination1");
        final EntitySeed seed3 = new EntitySeed("source2");
        final EntitySeed seed4 = new EntitySeed("destination2");

        final GetElementsWithinSet op = new GetElementsWithinSet(Arrays.asList(seed1, seed2, seed3, seed4));

        // When
        byte[] json = serialiser.serialise(op, true);

        final GetElementsWithinSet deserialisedOp = serialiser.deserialise(json, GetElementsWithinSet.class);

        // Then
        final Iterator itrSeedsA = deserialisedOp.getSeeds().iterator();
        assertEquals(seed1, itrSeedsA.next());
        assertEquals(seed2, itrSeedsA.next());
        assertEquals(seed3, itrSeedsA.next());
        assertEquals(seed4, itrSeedsA.next());
        assertFalse(itrSeedsA.hasNext());

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        GetElementsWithinSet getElementsWithinSet = new GetElementsWithinSet.Builder<>().addSeed(new EntitySeed("A")).includeEdges(GetOperation.IncludeEdgeType.NONE).includeEntities(true).option("testOption", "true").populateProperties(true).summarise(false).view(new View.Builder().edge("testEdgegroup").build()).build();
        assertEquals("true", getElementsWithinSet.getOption("testOption"));
        assertTrue(getElementsWithinSet.isIncludeEntities());
        assertEquals(GetOperation.IncludeEdgeType.NONE, getElementsWithinSet.getIncludeEdges());
        assertTrue(getElementsWithinSet.isPopulateProperties());
        assertFalse(getElementsWithinSet.isSummarise());
        assertEquals(new EntitySeed("A"), getElementsWithinSet.getInput().iterator().next());
        assertNotNull(getElementsWithinSet.getView());
    }
}
