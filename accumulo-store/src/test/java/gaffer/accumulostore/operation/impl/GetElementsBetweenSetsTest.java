package gaffer.accumulostore.operation.impl;

import gaffer.exception.SerialisationException;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.operation.OperationTest;
import gaffer.operation.data.EntitySeed;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
}
