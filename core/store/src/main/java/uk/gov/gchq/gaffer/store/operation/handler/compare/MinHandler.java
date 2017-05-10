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
import uk.gov.gchq.gaffer.data.element.comparison.ElementComparator;
import uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.compare.Min;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

public class MinHandler implements OutputOperationHandler<Min, Element> {
    @Override
    public Element doOperation(final Min operation, final Context context, final Store store) throws OperationException {
        // If the input or comparator is null, we return null
        if (null == operation.getInput() || null == operation.getComparator()) {
            return null;
        }

        final ElementComparator comparator = operation.getComparator();
        Stream<? extends Element> stream = Streams.toStream(operation.getInput());

        if (comparator instanceof ElementPropertyComparator) {
            final ElementPropertyComparator propertyComparator = (ElementPropertyComparator) comparator;
            stream = Streams.toStream(operation.getInput())
                            .filter(e -> e.getGroup() == propertyComparator.getGroupName())
                            .filter(e -> null != e.getProperty(propertyComparator.getPropertyName()));
        }

        return stream.min(comparator)
                     .orElseThrow(() -> new NoSuchElementException("Iterable was empty."));
    }
}
