package uk.gov.gchq.gaffer.accumulostore.operation.impl;


import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloTestData;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.OperationTest;
import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class GetEdgesBetweenSetsTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    public void shouldNotReturnEntities() {

        final GetEdgesBetweenSets op = new GetEdgesBetweenSets();
        op.setView(new View.Builder()
                .entity(TestGroups.ENTITY)
                .edge(TestGroups.EDGE)
                .build());
        assertFalse(op.getView().hasEntities());

    }

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final GetEdgesBetweenSets op = new GetEdgesBetweenSets(
                Arrays.asList(AccumuloTestData.SEED_SOURCE_1, AccumuloTestData.SEED_DESTINATION_1),
                Arrays.asList(AccumuloTestData.SEED_SOURCE_2, AccumuloTestData.SEED_DESTINATION_2));

        // When
        byte[] json = serialiser.serialise(op, true);

        final GetEdgesBetweenSets deserialisedOp = serialiser.deserialise(json, GetEdgesBetweenSets.class);

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
        final GetEdgesBetweenSets getEdgesBetweenSets = new GetEdgesBetweenSets.Builder()
                .directedType(GraphFilters.DirectedType.DIRECTED)
                .addSeed(AccumuloTestData.SEED_A)
                .addSeedB(AccumuloTestData.SEED_B)
                .inOutType(GraphFilters.IncludeIncomingOutgoingType.OUTGOING)
                .option(AccumuloTestData.TEST_OPTION_PROPERTY_KEY, "true")
                .view(new View.Builder()
                        .edge("testEdgeGroup")
                        .build())
                .build();
        assertEquals(GraphFilters.DirectedType.DIRECTED, getEdgesBetweenSets.getDirectedType());
        assertEquals(GraphFilters.IncludeIncomingOutgoingType.OUTGOING, getEdgesBetweenSets.getIncludeIncomingOutGoing());
        assertEquals("true", getEdgesBetweenSets.getOption(AccumuloTestData.TEST_OPTION_PROPERTY_KEY));
        assertEquals(AccumuloTestData.SEED_A, getEdgesBetweenSets.getSeeds().iterator().next());
        assertEquals(AccumuloTestData.SEED_B, getEdgesBetweenSets.getSeedsB().iterator().next());
        assertNotNull(getEdgesBetweenSets.getView());
    }
}
