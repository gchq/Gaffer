/*
 * Copyright 2020 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.gov.gchq.gaffer.store.operation.handler;

import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.GenerateSplitPointsFromSample;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractGenerateSplitPointsFromSampleHandler<T, S extends Store> implements OutputOperationHandler<GenerateSplitPointsFromSample<T>, List<T>> {

    @Override
    public List<T> doOperation(final GenerateSplitPointsFromSample<T> operation, final Context context, final Store store) throws OperationException {

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

    protected void validate(final GenerateSplitPointsFromSample operation, final S store) throws OperationException {
        if (null == operation.getInput()) {
            throw new OperationException("Operation input is required.");
        }
    }

    protected Integer getNumSplits(final GenerateSplitPointsFromSample operation, final S typedStore) {
        return operation.getNumSplits();
    }

}
