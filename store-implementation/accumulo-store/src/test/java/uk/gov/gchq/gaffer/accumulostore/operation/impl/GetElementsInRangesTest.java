package uk.gov.gchq.gaffer.accumulostore.operation.impl;


import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloTestData;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class GetElementsInRangesTest extends OperationTest<GetElementsInRanges> {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final List<Pair<ElementId, ElementId>> pairList = new ArrayList<>();
        final Pair<ElementId, ElementId> pair1 = new Pair<>(AccumuloTestData.SEED_SOURCE_1, AccumuloTestData.SEED_DESTINATION_1);
        final Pair<ElementId, ElementId> pair2 = new Pair<>(AccumuloTestData.SEED_SOURCE_2, AccumuloTestData.SEED_DESTINATION_2);
        pairList.add(pair1);
        pairList.add(pair2);
        final GetElementsInRanges op = new GetElementsInRanges.Builder()
                .input(pairList)
                .build();
        // When
        byte[] json = serialiser.serialise(op, true);

        final GetElementsInRanges deserialisedOp = serialiser.deserialise(json, GetElementsInRanges.class);

        // Then
        final Iterator<? extends Pair<? extends ElementId, ? extends ElementId>> itrPairs = deserialisedOp.getInput().iterator();
        assertEquals(pair1, itrPairs.next());
        assertEquals(pair2, itrPairs.next());
        assertFalse(itrPairs.hasNext());

    }

    @SuppressWarnings("unchecked")
    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final Pair<ElementId, ElementId> seed = new Pair<>(AccumuloTestData.SEED_A, AccumuloTestData.SEED_B);
        final GetElementsInRanges getElementsInRanges = new GetElementsInRanges.Builder()
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER)
                .input(seed)
                .directedType(DirectedType.UNDIRECTED)
                .option(AccumuloTestData.TEST_OPTION_PROPERTY_KEY, "true")
                .view(new View.Builder().edge("testEdgeGroup").build())
                .build();
        assertEquals("true", getElementsInRanges.getOption(AccumuloTestData.TEST_OPTION_PROPERTY_KEY));
        assertEquals(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER, getElementsInRanges.getIncludeIncomingOutGoing());
        assertEquals(DirectedType.UNDIRECTED, getElementsInRanges.getDirectedType());
        assertEquals(seed, getElementsInRanges.getInput().iterator().next());
        assertNotNull(getElementsInRanges.getView());
    }

    @Override
    protected GetElementsInRanges getTestObject() {
        return new GetElementsInRanges();
    }
}
