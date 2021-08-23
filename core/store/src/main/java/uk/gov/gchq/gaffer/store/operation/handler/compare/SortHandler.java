/*
 * Copyright 2017-2020 Crown Copyright
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
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.FieldDeclaration;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.util.Collections;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Objects.isNull;
import static uk.gov.gchq.gaffer.operation.impl.compare.ElementComparisonUtil.entryComparators;
import static uk.gov.gchq.gaffer.operation.impl.compare.ElementComparisonUtil.getCombinedComparator;
import static uk.gov.gchq.gaffer.operation.impl.compare.ElementComparisonUtil.getComparators;

/**
 * A {@code SortHandler} handles the Sort operation. It does that
 * in memory using the {@link uk.gov.gchq.gaffer.commonutil.iterable.LimitedInMemorySortedIterable}.
 * If the resultLimit is set to one that it just deletes the operation to the
 * {@link MaxHandler}.
 */
public class SortHandler implements OperationHandler<Iterable<? extends Element>> {
    private static final MaxHandler MAX_HANDLER = new MaxHandler();
    public static final String DEDUPLICATE = "deduplicate";
    public static final String RESULT_LIMIT = "resultLimit";
    public static final String INPUT = "input";

    @Override
    public Iterable<? extends Element> _doOperation(final Operation operation, final Context context, final Store store) throws OperationException {
        // If there is no input or there are no comparators, we return null
        Iterable input = (Iterable) operation.input();
        if (isNull(input)
                || isNull(getComparators(operation))
                || getComparators(operation).isEmpty()) {
            return null;
        }

        // If the result limit is set to 1 then we can just delegate the operation to the MaxHandler.
        if (null != operation.get(RESULT_LIMIT) && 1 == (Integer) operation.get(RESULT_LIMIT)) {
            final Element max = MAX_HANDLER.doOperation(new MaxHandler.Builder()
                    .comparators(getComparators(operation))
                    .input(input)
                    .build(), context, store);
            if (null == max) {
                return Collections.emptyList();
            }
            return Collections.singletonList(max);
        }

        try (final Stream<? extends Element> stream =
                     Streams.toStream(input)
                             .filter(Objects::nonNull)) {
            return stream.collect(
                    GafferCollectors.toLimitedInMemorySortedIterable(
                            getCombinedComparator(operation),
                            (Integer) operation.get(RESULT_LIMIT),
                            (Boolean) operation.get(DEDUPLICATE)
                    )
            );
        } finally {
            CloseableUtil.close(operation);
        }
    }

    @Override
    public FieldDeclaration getFieldDeclaration() {
        return new FieldDeclaration()
                .fieldRequired(entryComparators)
                .fieldRequired(INPUT, Iterable.class)
                .fieldOptional(RESULT_LIMIT, Integer.class)
                .fieldOptional(DEDUPLICATE, Boolean.class);
    }

    static class Builder extends OperationHandler.BuilderSpecificInputOperation<Builder> {
        @Override
        protected Builder getBuilder() {
            return this;
        }

        @Override
        protected FieldDeclaration getFieldDeclaration() {
            return new SortHandler().getFieldDeclaration();
        }
    }
}
