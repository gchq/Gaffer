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
package uk.gov.gchq.gaffer.mapstore.impl;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterator;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.utils.ElementCloner;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static uk.gov.gchq.gaffer.mapstore.impl.MapImpl.COUNT;

/**
 * An {@link OperationHandler} for the {@link GetAllElements} operation on the {@link MapStore}.
 */
public class GetAllElementsHandler implements OperationHandler<GetAllElements<Element>, CloseableIterable<Element>> {

    @Override
    public CloseableIterable<Element> doOperation(final GetAllElements<Element> operation,
                                                  final Context context,
                                                  final Store store) throws OperationException {
        return doOperation(operation, (MapStore) store);
    }

    private CloseableIterable<Element> doOperation(final GetAllElements<Element> operation, final MapStore mapStore) {
        return new AllElementsIterable(mapStore.getMapImpl(), operation);
    }

    private static class AllElementsIterable extends WrappedCloseableIterable<Element> {
        private final MapImpl mapImpl;
        private final GetAllElements getAllElements;

        AllElementsIterable(final MapImpl mapImpl, final GetAllElements getAllElements) {
            this.mapImpl = mapImpl;
            this.getAllElements = getAllElements;
        }

        @Override
        public CloseableIterator<Element> iterator() {
            // Create stream of elements from elementToProperties by copying the properties from the value into the key
            Stream<Element> elements = mapImpl.elementToProperties.entrySet()
                    .stream()
                    .map(x -> {
                        final Element element = x.getKey();
                        final Properties properties = x.getValue();
                        if (mapImpl.groupsWithNoAggregation.contains(x.getKey().getGroup())) {
                            final int count = (int) properties.get(COUNT);
                            List<Element> duplicateElements = new ArrayList<>(count);
                            IntStream.range(0, count).forEach(i -> duplicateElements.add(element));
                            return duplicateElements;
                        } else {
                            element.copyProperties(properties);
                            return Collections.singletonList(element);
                        }
                    })
                    .flatMap(x -> x.stream());
            final Stream<Element> elementsAfterIncludeEntitiesEdgesOption = GetElementsHandler.
                    applyIncludeEntitiesEdgesOptions(elements, getAllElements.isIncludeEntities(),
                            getAllElements.getIncludeEdges());
            final Stream<Element> afterView = GetElementsHandler
                    .applyView(elementsAfterIncludeEntitiesEdgesOption, mapImpl.schema, getAllElements.getView());
            final Stream<Element> clonedElements = afterView.map(element -> ElementCloner.cloneElement(element, mapImpl.schema));
            if (!getAllElements.isPopulateProperties()) {
                // If populateProperties option is false then remove all properties
                return new WrappedCloseableIterator<>(clonedElements.map(e -> e.emptyClone()).iterator());
            }
            return new WrappedCloseableIterator<>(clonedElements.iterator());
        }
    }
}
