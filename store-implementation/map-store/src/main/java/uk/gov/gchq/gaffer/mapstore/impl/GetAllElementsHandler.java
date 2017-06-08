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
import uk.gov.gchq.gaffer.data.element.GroupedProperties;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

/**
 * An {@link OutputOperationHandler} for the {@link GetAllElements} operation on the {@link MapStore}.
 */
public class GetAllElementsHandler implements OutputOperationHandler<GetAllElements, CloseableIterable<? extends Element>> {

    @Override
    public CloseableIterable<? extends Element> doOperation(final GetAllElements operation,
                                                            final Context context,
                                                            final Store store) throws OperationException {
        return doOperation(operation, (MapStore) store);
    }

    private CloseableIterable<Element> doOperation(final GetAllElements operation, final MapStore mapStore) {
        return new AllElementsIterable(mapStore.getMapImpl(), operation, mapStore.getSchema());
    }

    private static class AllElementsIterable extends WrappedCloseableIterable<Element> {
        private final MapImpl mapImpl;
        private final GetAllElements getAllElements;
        private final Schema schema;

        AllElementsIterable(final MapImpl mapImpl, final GetAllElements getAllElements, final Schema schema) {
            this.mapImpl = mapImpl;
            this.getAllElements = getAllElements;
            this.schema = schema;
        }

        @Override
        public CloseableIterator<Element> iterator() {
            // Create stream of elements from aggElements by copying the properties from the value into the key
            Stream<Map.Entry<Element, GroupedProperties>> aggElementEntries = mapImpl.aggElements.values().stream()
                    .flatMap(map -> map.entrySet().stream());
            Stream<Element> aggElements = aggElementEntries
                    .map(x -> {
                        final Element element = x.getKey();
                        final Properties properties = x.getValue();
                        element.copyProperties(properties);
                        return element;
                    });

            Stream<Map.Entry<Element, Integer>> nonAggElementEntries = mapImpl.nonAggElements.values().stream()
                    .flatMap(map -> map.entrySet().stream());
            Stream<Element> nonAggElements = nonAggElementEntries
                    .map(x -> {
                        final Element element = x.getKey();
                        final int count = x.getValue();
                        return Collections.nCopies(count, element);
                    })
                    .flatMap(Collection::stream);

            final Stream<Element> elements = Stream.concat(aggElements, nonAggElements);

            final Stream<Element> elementsAfterIncludeEntitiesEdgesOption =
                    GetElementsHandler.applyIncludeEntitiesEdgesOptions(elements, getAllElements.getView().hasEntities(),
                            getAllElements.getView().hasEdges(), getAllElements.getDirectedType());
            final Stream<Element> afterView = GetElementsHandler
                    .applyView(elementsAfterIncludeEntitiesEdgesOption, schema, getAllElements.getView());
            final Stream<Element> clonedElements = afterView.map(element -> mapImpl.mapFactory.cloneElement(element, schema));
            return new WrappedCloseableIterator<>(clonedElements.iterator());
        }
    }
}
