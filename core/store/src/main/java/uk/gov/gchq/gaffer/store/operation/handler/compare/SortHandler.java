/*
 * Copyright 2017 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.stream.GafferCollectors;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.compare.Sort;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import java.util.Comparator;

public class SortHandler implements OutputOperationHandler<Sort, Iterable<? extends Element>> {
    @Override
    public Iterable<Element> doOperation(final Sort operation, final Context context, final Store store) throws OperationException {

        // If the input is null, we return null
        if (null == operation.getInput()) {
            return null;
        }

        // If propertyName and the property comparator are both non-null, we sort based on a property comparison
        if (null != operation.getPropertyName() && null != operation.getPropertyComparator()) {
            final Comparator<Object> comparator = operation.isReversed()
                    ? operation.getPropertyComparator().reversed()
                    : operation.getPropertyComparator();

            return Streams.toStream(operation.getInput())
                          .filter(e -> null != e.getProperty(operation.getPropertyName()))
                          .sorted(Comparator.comparing(e -> e.getProperty(operation
                                  .getPropertyName()), comparator))
                          .limit(operation.getResultLimit())
                          .collect(GafferCollectors.toCloseableIterable());
        }

        // If the element comparator is non-null, then we carry out an element comparison
        if (null != operation.getElementComparator()) {
            final Comparator<Element> comparator = operation.isReversed()
                    ? operation.getElementComparator().reversed()
                    : operation.getElementComparator();

            return Streams.toStream(operation.getInput())
                          .sorted(comparator)
                          .limit(operation.getResultLimit())
                          .collect(GafferCollectors.toCloseableIterable());
        }

        // If both comparators are null, we return null
        return null;
    }
}
