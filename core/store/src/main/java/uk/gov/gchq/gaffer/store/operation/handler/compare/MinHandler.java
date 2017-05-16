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
import uk.gov.gchq.gaffer.data.element.comparison.ElementComparator;
import uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.compare.Min;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

public class MinHandler implements OutputOperationHandler<Min, Element> {
    @Override
    public Element doOperation(final Min operation, final Context context, final Store store) throws OperationException {
        // If the input or comparator is null, we return null
        if (null == operation.getInput() || null == operation.getComparator()) {
            return null;
        }

        final ElementComparator comparator = operation.getComparator();
        try {
            return getMin(operation.getInput(), comparator);
        } finally {
            CloseableUtil.close(operation);
        }
    }

    private Element getMin(final Iterable<? extends Element> elements, final ElementComparator comparator) {
        Element minElement = null;

        if (comparator instanceof ElementPropertyComparator) {
            final ElementPropertyComparator propertyComparator = (ElementPropertyComparator) comparator;
            Object minProperty = null;
            for (final Element element : elements) {
                if (null == element || !element.getGroup().equals(propertyComparator.getGroupName())) {
                    continue;
                }
                final Object property = element.getProperty(propertyComparator.getPropertyName());
                if (null == property) {
                    continue;
                }
                if (null == minElement || propertyComparator._compare(property, minProperty) < 0) {
                    minElement = element;
                    minProperty = property;
                }
            }
        } else {
            for (final Element element : elements) {
                if (null == element) {
                    continue;
                }
                if (null == minElement) {
                    minElement = element;
                }
                if (comparator.compare(element, minElement) < 0) {
                    minElement = element;
                }
            }
        }

        return minElement;
    }
}
