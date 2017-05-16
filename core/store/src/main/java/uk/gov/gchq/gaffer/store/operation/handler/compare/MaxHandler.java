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

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.compare.Max;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import java.util.Comparator;

public class MaxHandler implements OutputOperationHandler<Max, Element> {
    @Override
    public Element doOperation(final Max operation, final Context context, final Store store) throws OperationException {
        // If the input or comparator is null, we return null
        if (null == operation.getInput() || null == operation.getComparator()) {
            return null;
        }

        final Comparator<Element> comparator = operation.getComparator();
        try {
            return getMax(operation.getInput(), comparator);
        } finally {
            CloseableUtil.close(operation);
        }
    }

    private Element getMax(final Iterable<? extends Element> elements, final Comparator<Element> comparator) {
        Element maxElement = null;

        if (comparator instanceof ElementPropertyComparator) {
            final ElementPropertyComparator propertyComparator = (ElementPropertyComparator) comparator;
            Object maxProperty = null;
            for (final Element element : elements) {
                if (null == element || !element.getGroup().equals(propertyComparator.getGroupName())) {
                    continue;
                }
                final Object property = element.getProperty(propertyComparator.getPropertyName());
                if (null == property) {
                    continue;
                }
                if (null == maxElement || propertyComparator._compare(property, maxProperty) > 0) {
                    maxElement = element;
                    maxProperty = property;
                }
            }
        } else {
            for (final Element element : elements) {
                if (null == element) {
                    continue;
                }
                if (null == maxElement) {
                    maxElement = element;
                }
                if (comparator.compare(element, maxElement) > 0) {
                    maxElement = element;
                }
            }
        }

        return maxElement;
    }
}
