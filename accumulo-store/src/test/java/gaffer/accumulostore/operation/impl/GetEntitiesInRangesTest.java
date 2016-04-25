package gaffer.accumulostore.operation.impl;


import gaffer.accumulostore.utils.Pair;
import gaffer.data.elementdefinition.view.View;
import gaffer.exception.SerialisationException;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.operation.GetOperation;
import gaffer.operation.OperationTest;
import gaffer.operation.data.EntitySeed;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class GetEntitiesInRangesTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    public void shouldNotReturnEdges(){

        final GetEntitiesInRanges<Pair<EntitySeed>> op = new GetEntitiesInRanges<>();
        assertEquals(GetOperation.IncludeEdgeType.NONE, op.getIncludeEdges());

    }

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        List<Pair<EntitySeed>> pairList = new ArrayList<>();
        Pair<EntitySeed> pair1 = new Pair<>(new EntitySeed("source1"), new EntitySeed("destination1"));
        Pair<EntitySeed> pair2 = new Pair<>(new EntitySeed("source2"), new EntitySeed("destination2"));
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
        Pair<EntitySeed> seed = new Pair<>(new EntitySeed("A"), new EntitySeed("B"));
        GetEntitiesInRanges getEntitiesInRanges = new GetEntitiesInRanges.Builder<Pair<EntitySeed>>()
                .option("testOption", "true")
                .populateProperties(false)
                .summarise(true)
                .view(new View.Builder().edge("testEdgeGroup").build())
                .addSeed(seed).build();
        assertEquals(seed, getEntitiesInRanges.getSeeds().iterator().next());
        assertEquals("true", getEntitiesInRanges.getOption("testOption"));
        assertTrue(getEntitiesInRanges.isSummarise());
        assertFalse(getEntitiesInRanges.isPopulateProperties());
        assertNotNull(getEntitiesInRanges.getView());
    }
}
