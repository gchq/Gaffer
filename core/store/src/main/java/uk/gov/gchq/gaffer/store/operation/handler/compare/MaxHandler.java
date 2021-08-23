/*
 * Copyright 2017-2021 Crown Copyright
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

import static java.util.Objects.*;
import static uk.gov.gchq.gaffer.operation.impl.compare.ElementComparisonUtil.entryComparators;
import static uk.gov.gchq.gaffer.operation.impl.compare.ElementComparisonUtil.getCombinedComparator;
import static uk.gov.gchq.gaffer.operation.impl.compare.ElementComparisonUtil.getComparators;

/**
 * A {@code MaxHandler} handles the Max operation.
 * <p>
 * It uses the {@link Comparator}s instances on the operation to determine the
 * object with the maximum value.
 */
public class MaxHandler implements OperationHandler<Element> {
    @Override
    public Element _doOperation(final Operation operation, final Context context, final Store store) throws OperationException {
        // If there is no input or there are no comparators, we return null
        if (null == operation.input()
                || isNull( getComparators(operation))
                || getComparators(operation).isEmpty()) {
            return null;
        }

        try {
            return getMax((Iterable<? extends Element>) operation.input(), operation);
        } finally {
            CloseableUtil.close(operation);
        }
    }


    @Override
    public FieldDeclaration getFieldDeclaration() {
        return new FieldDeclaration()
                .fieldRequired(entryComparators)
                .fieldRequired("input", Iterable.class);

    }

    private Element getMax(final Iterable<? extends Element> elements, final Operation operation) {
        Element maxElement = null;

        final List<Comparator<Element>> comparators = getComparators(operation);
        if (1 == comparators.size() && comparators.get(0) instanceof ElementPropertyComparator) {
            final ElementPropertyComparator propertyComparator = (ElementPropertyComparator) comparators.get(0);
            Object maxProperty = null;
            for (final Element element : elements) {
                if (null == element || !propertyComparator.getGroups().contains(element.getGroup())) {
                    continue;
                }
                final Object property = element.getProperty(propertyComparator.getProperty());
                if (null == property) {
                    continue;
                }
                if (null == maxElement || propertyComparator._compare(property, maxProperty) > 0) {
                    maxElement = element;
                    maxProperty = property;
                }
            }
        } else {
            final Comparator<Element> combinedComparator = getCombinedComparator(operation);
            if (null != combinedComparator) {
                for (final Element element : elements) {
                    if (null == element) {
                        continue;
                    }
                    if (null == maxElement) {
                        maxElement = element;
                    }
                    if (combinedComparator.compare(element, maxElement) > 0) {
                        maxElement = element;
                    }
                }
            }
        }

        return maxElement;
    }


    static class Builder extends BuilderSpecificInputOperation<Builder> {

        public Builder comparators(List<Comparator<Element>> comparators) {
            operation.operationArg(ElementComparisonUtil.KEY_COMPARATORS, comparators);
            return this;
        }

        public Builder comparators(Comparator<Element>... comparators) {
            operation.operationArg(ElementComparisonUtil.KEY_COMPARATORS, Lists.newArrayList(comparators));
            return this;
        }

        @Override
        protected Builder getBuilder() {
            return this;
        }

        @Override
        protected FieldDeclaration getFieldDeclaration() {
            return new MaxHandler().getFieldDeclaration();
        }
    }
}
