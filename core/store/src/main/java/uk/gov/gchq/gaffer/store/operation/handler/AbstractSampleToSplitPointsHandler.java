package uk.gov.gchq.gaffer.store.operation.handler;

import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SampleToSplitPoints;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractSampleToSplitPointsHandler <T, S extends Store> implements OutputOperationHandler<SampleToSplitPoints<T>, List<T>> {

    @Override
    public List<T> doOperation(final SampleToSplitPoints<T> operation, final Context context, final Store store) throws OperationException {

        final S typedStore = (S) store;

        validate(operation, typedStore);

        final Integer numSplits = getNumSplits(operation, typedStore);
        if (null == numSplits) {
            throw new OperationException("Number of splits is required");
        }
        if (numSplits < 1) {
            return Collections.emptyList();
        }

        final List<T> records = Streams.toStream(operation.getInput()).collect(Collectors.toList());

        final List<T> splits;
        if (records.size() < 2 || records.size() <= numSplits) {
            splits = records;
        } else {
            final LinkedHashSet<T> splitsSet = Integer.MAX_VALUE != numSplits ? new LinkedHashSet<>(numSplits) : new LinkedHashSet<>();
            final double outputEveryNthRecord = ((double) records.size()) / (numSplits + 1);
            int nthCount = 0;
            for (final T record : records) {
                nthCount++;
                if (nthCount >= (int) (outputEveryNthRecord * (splitsSet.size() + 1))) {
                    splitsSet.add(record);
                    if (numSplits == splitsSet.size()) {
                        break;
                    }
                }
            }

            splits = new ArrayList<>(splitsSet);
        }

        return splits;
    }

    protected void validate(final SampleToSplitPoints operation, final S store) throws OperationException {
        if (null == operation.getInput()) {
            throw new OperationException("Operation input is required.");
        }
    }

    protected Integer getNumSplits(final SampleToSplitPoints operation, final S typedStore) {
        return operation.getNumSplits();
    }

}
