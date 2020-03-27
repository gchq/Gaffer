package uk.gov.gchq.gaffer.store.operation.handler;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SampleToSplitPoints;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractSampleToSplitPointsHandlerTest<S extends Store> {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected Schema schema = new Schema.Builder().build();

    @Test
    public void shouldThrowExceptionForNullInput() throws OperationException {
        // Given
        final AbstractSampleToSplitPointsHandler<?, S> handler = createHandler();
        final SampleToSplitPoints operation = new SampleToSplitPoints.Builder<>()
                .numSplits(1)
                .build();

        expectedException.expect(OperationException.class);
        expectedException.expectMessage("input is required");

        handler.doOperation(operation, new Context(), createStore());
    }


    @Test
    public void shouldReturnEmptyCollectionIfNumSplitsIsLessThan1() throws OperationException {
        // Given
        final List<String> sample = createSampleOfSize(100);
        final AbstractSampleToSplitPointsHandler<?, S> handler = createHandler();
        final SampleToSplitPoints operation = new SampleToSplitPoints.Builder<>()
                .input(sample)
                .numSplits(0)
                .build();

        // When
        final List<?> splits = handler.doOperation(operation, new Context(), createStore());

        // Then
        assertTrue(splits.isEmpty());
    }


    @Test
    public void shouldCalculateRequiredNumberOfSplits() throws OperationException {

        // Given
        final int numSplits = 3;
        final List<String> sample = createSampleOfSize(numSplits * 10);

        final AbstractSampleToSplitPointsHandler<?, S> handler = createHandler();
        final SampleToSplitPoints operation = new SampleToSplitPoints.Builder<>()
                .input(sample)
                .numSplits(numSplits)
                .build();

        // When
        final List<?> splits = handler.doOperation(operation, new Context(), createStore());

        // Then
        verifySplits(Arrays.asList(6, 14, 21), sample, splits, handler);
    }

    protected abstract S createStore();

    protected abstract AbstractSampleToSplitPointsHandler<?, S> createHandler();

    protected void verifySplits(final List<Integer> indexes, final List<String> sample, final List<?> splits, final AbstractSampleToSplitPointsHandler<?, S> handler) throws OperationException {
        final SampleToSplitPoints operatation = new SampleToSplitPoints.Builder<>()
                .input(sample)
                .numSplits(Integer.MAX_VALUE)
                .build();
        final List<?> allElementsAsSplits = handler.doOperation(operatation, new Context(), createStore());

        final List<Object> expectedSplits = new ArrayList<>(indexes.size());
        for (final Integer index : indexes) {
            expectedSplits.add(allElementsAsSplits.get(index));
        }

        assertEquals(expectedSplits, splits);
    }

    private List<String> createSampleOfSize(final int size) {

        return IntStream.range(0, size).mapToObj(Integer::toString).map("key"::concat).collect(Collectors.toList());
    }
}
