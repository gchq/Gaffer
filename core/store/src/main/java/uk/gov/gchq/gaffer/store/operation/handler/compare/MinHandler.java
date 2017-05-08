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

import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.compare.Min;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import java.util.NoSuchElementException;

public class MinHandler implements OutputOperationHandler<Min, Element> {
    @Override
    public Element doOperation(final Min operation, final Context context, final Store store) throws OperationException {
        // If the input is null, we return null
        if (null == operation.getInput()) {
            return null;
        }

        // If propertyName and the property comparator are both non-null, we carry out a property comparison
        if (null != operation.getPropertyName() && null != operation.getPropertyComparator()) {
            return Streams.toStream(operation.getInput())
                          .filter(e -> null != e.getProperty(operation.getPropertyName()))
                          .reduce((l, r) ->
                                  operation.getPropertyComparator()
                                           .compare(l.getProperty(operation.getPropertyName()),
                                                   r.getProperty(operation.getPropertyName())) < 0 ? l : r)
                          .orElseThrow(() -> new NoSuchElementException("Iterable was empty."));
        }

        // If the element comparator is non-null, then we carry out an element comparison
        if (null != operation.getElementComparator()) {
            return Streams.toStream(operation.getInput())
                          .min(operation.getElementComparator())
                          .orElseThrow(() -> new NoSuchElementException("Iterable was empty."));
        }

        // If both comparators are null, we return null
        return null;
    }
}
