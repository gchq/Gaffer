/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.store.operation.handler.compare;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.stream.GafferCollectors;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.compare.Max;
import uk.gov.gchq.gaffer.operation.impl.compare.Sort;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.Collections;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * A {@code SortHandler} handles the {@link Sort} operation. It does that
 * in memory using the {@link uk.gov.gchq.gaffer.commonutil.iterable.LimitedInMemorySortedIterable}.
 * If the resultLimit is set to one that it just deletes the operation to the
 * {@link MaxHandler}.
 */
public class SortHandler implements OutputOperationHandler<Sort, Iterable<? extends Element>> {
    private static final MaxHandler MAX_HANDLER = new MaxHandler();

    @Override
    public Iterable<? extends Element> doOperation(final Sort operation, final Context context, final Store store) throws OperationException {
        // If there is no input or there are no comparators, we return null
        if (null == operation.getInput()
                || null == operation.getComparators()
                || operation.getComparators().isEmpty()) {
            return null;
        }

        // If the result limit is set to 1 then we can just delegate the operation to the MaxHandler.
        if (null != operation.getResultLimit() && 1 == operation.getResultLimit()) {
            final Element max = MAX_HANDLER.doOperation(new Max.Builder()
                    .comparators(operation.getComparators())
                    .input(operation.getInput())
                    .build(), context, store);
            if (null == max) {
                return Collections.emptyList();
            }
            return Collections.singletonList(max);
        }

        try (final Stream<? extends Element> stream =
                     Streams.toStream(operation.getInput())
                             .filter(Objects::nonNull)) {
            return stream.collect(
                    GafferCollectors.toLimitedInMemorySortedIterable(
                            operation.getCombinedComparator(),
                            operation.getResultLimit(),
                            operation.isDeduplicate()
                    )
            );
        } finally {
            CloseableUtil.close(operation);
        }
    }
}
