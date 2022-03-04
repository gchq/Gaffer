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

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.comparison.ElementPropertyComparator;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.compare.ElementComparisonUtil;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.FieldDeclaration;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.util.Comparator;
import java.util.List;

import static uk.gov.gchq.gaffer.operation.impl.compare.ElementComparisonUtil.fieldComparators;
import static uk.gov.gchq.gaffer.operation.impl.compare.ElementComparisonUtil.getComparators;

/**
 * A {@code MinHandler} handles the Min operation.
 * <p>
 * It uses the {@link Comparator}s instances on the operation to determine the
 * object with the minimum value.
 */
public class MinHandler implements OperationHandler<Element> {

    public Element _doOperation(Operation operation, Context context, Store store) throws OperationException {
        List<Comparator<Element>> comparators = getComparators(operation);
        Iterable<? extends Element> input = (Iterable<? extends Element>) operation.input();

        // If there is no input or there are no comparators, we return null
        if (null == input
                || null == comparators
                || comparators.isEmpty()) {
            return null;
        }

        try {
            return getMin(input, operation);
        } finally {
            CloseableUtil.close(operation);
        }
    }

    @Override
    public FieldDeclaration getFieldDeclaration() {
        return new FieldDeclaration()
                .inputOptional(Iterable.class)
                .fieldOptional(fieldComparators);
    }

    private Element getMin(final Iterable<? extends Element> elements, final Operation operation) {
        Element minElement = null;

        final List<Comparator<Element>> comparators = getComparators(operation);
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
            final Comparator<Element> combinedComparator = ElementComparisonUtil.getCombinedComparator(operation);
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

    static class OperationBuilder extends BuilderSpecificInputOperation<OperationBuilder,MinHandler> {

        public OperationBuilder comparators(final List<Comparator<Element>> comparators) {
            operation.operationArg(ElementComparisonUtil.KEY_COMPARATORS, comparators);
            return this;
        }

        public OperationBuilder comparators(Comparator<Element>... comparators) {
            operation.operationArg(ElementComparisonUtil.KEY_COMPARATORS, Lists.newArrayList(comparators));
            return this;
        }

        @Override
        protected OperationBuilder getBuilder() {
            return this;
        }

        @Override
        protected MinHandler getHandler() {
            return new MinHandler();
        }
    }
}
