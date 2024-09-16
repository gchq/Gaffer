/*
 * Copyright 2017-2023 Crown Copyright
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
import uk.gov.gchq.gaffer.operation.impl.compare.Min;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.Comparator;
import java.util.List;

/**
 * A {@code MinHandler} handles the {@link Min} operation.
 *
 * It uses the {@link Comparator}s instances on the operation to determine the
 * object with the minimum value.
 */
public class MinHandler implements OutputOperationHandler<Min, Element> {
    @Override
    @SuppressWarnings("PMD.UseTryWithResources")
    public Element doOperation(final Min operation, final Context context, final Store store) throws OperationException {
        // If there is no input or there are no comparators, we return null
        if (null == operation.getInput()
                || null == operation.getComparators()
                || operation.getComparators().isEmpty()) {
            return null;
        }

        try {
            return getMin(operation.getInput(), operation);
        } finally {
            CloseableUtil.close(operation);
        }
    }

    private Element getMin(final Iterable<? extends Element> elements, final Min operation) {
        Element minElement = null;

        final List<Comparator<Element>> comparators = operation.getComparators();
        if (1 == comparators.size() && comparators.get(0) instanceof ElementPropertyComparator) {
            final ElementPropertyComparator propertyComparator = (ElementPropertyComparator) comparators.get(0);
            Object minProperty = null;
            for (final Element element : elements) {
                if (null == element || !propertyComparator.getGroups().contains(element.getGroup())) {
                    continue;
                }
                final Object property = element.getProperty(propertyComparator.getProperty());
                if (null == property) {
                    continue;
                }
                if (null == minElement || propertyComparator._compare(property, minProperty) < 0) {
                    minElement = element;
                    minProperty = property;
                }
            }
        } else {
            final Comparator<Element> combinedComparator = operation.getCombinedComparator();
            if (null != combinedComparator) {
                for (final Element element : elements) {
                    if (null == element) {
                        continue;
                    }
                    if (null == minElement) {
                        minElement = element;
                    }
                    if (combinedComparator.compare(element, minElement) < 0) {
                        minElement = element;
                    }
                }
            }
        }

        return minElement;
    }
}
