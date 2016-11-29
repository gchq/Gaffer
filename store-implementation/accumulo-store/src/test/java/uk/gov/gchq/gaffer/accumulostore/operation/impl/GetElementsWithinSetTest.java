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

public class GetElementsWithinSetTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final GetElementsWithinSet op = new GetElementsWithinSet(Arrays.asList(AccumuloTestData.SEED_SOURCE_1,
                AccumuloTestData.SEED_DESTINATION_1, AccumuloTestData.SEED_SOURCE_2, AccumuloTestData.SEED_DESTINATION_2));

        // When
        byte[] json = serialiser.serialise(op, true);

        final GetElementsWithinSet deserialisedOp = serialiser.deserialise(json, GetElementsWithinSet.class);

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
        final GetElementsWithinSet getElementsWithinSet = new GetElementsWithinSet.Builder<>().addSeed(AccumuloTestData.SEED_A)
                .includeEdges(GetOperation.IncludeEdgeType.NONE).includeEntities(true).option(AccumuloTestData.TEST_OPTION_PROPERTY_KEY, "true")
                .populateProperties(true).view(new View.Builder().edge("testEdgegroup").build()).build();
        assertEquals("true", getElementsWithinSet.getOption(AccumuloTestData.TEST_OPTION_PROPERTY_KEY));
        assertTrue(getElementsWithinSet.isIncludeEntities());
        assertEquals(GetOperation.IncludeEdgeType.NONE, getElementsWithinSet.getIncludeEdges());
        assertTrue(getElementsWithinSet.isPopulateProperties());
        assertEquals(AccumuloTestData.SEED_A, getElementsWithinSet.getInput().iterator().next());
        assertNotNull(getElementsWithinSet.getView());
    }
}
