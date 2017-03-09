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
import static org.junit.Assert.fail;

public class GetEdgesWithinSetTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    public void shouldNotReturnEntities() {

        final GetEdgesWithinSet op = new GetEdgesWithinSet();
        assertFalse(op.isIncludeEntities());

    }

    @Test
    public void shouldNotBeAbleToSetNoEdges() {

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
                Arrays.asList(AccumuloTestData.SEED_SOURCE_1, AccumuloTestData.SEED_DESTINATION_1,
                        AccumuloTestData.SEED_SOURCE_2, AccumuloTestData.SEED_DESTINATION_2));

        // When
        byte[] json = serialiser.serialise(op, true);

        final GetEdgesWithinSet deserialisedOp = serialiser.deserialise(json, GetEdgesWithinSet.class);

        // Then
        final Iterator itrSeedsA = deserialisedOp.getSeeds().iterator();
        assertEquals(AccumuloTestData.SEED_SOURCE_1, itrSeedsA.next());
        assertEquals(AccumuloTestData.SEED_DESTINATION_1, itrSeedsA.next());
        assertEquals(AccumuloTestData.SEED_SOURCE_2, itrSeedsA.next());
        assertEquals(AccumuloTestData.SEED_DESTINATION_2, itrSeedsA.next());
        assertFalse(itrSeedsA.hasNext());
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final GetEdgesWithinSet getEdgesWithinSet = new GetEdgesWithinSet.Builder()
                .includeEdges(GetOperation.IncludeEdgeType.DIRECTED)
                .addSeed(AccumuloTestData.SEED_A).option(AccumuloTestData.TEST_OPTION_PROPERTY_KEY, "true").populateProperties(false)
                .view(new View.Builder().edge("testEdgeGroup").build()).build();
        assertFalse(getEdgesWithinSet.isPopulateProperties());
        assertEquals(GetOperation.IncludeEdgeType.DIRECTED, getEdgesWithinSet.getIncludeEdges());
        assertEquals("true", getEdgesWithinSet.getOption(AccumuloTestData.TEST_OPTION_PROPERTY_KEY));
        assertEquals(AccumuloTestData.SEED_A, getEdgesWithinSet.getSeeds().iterator().next());
        assertNotNull(getEdgesWithinSet.getView());
    }
}
