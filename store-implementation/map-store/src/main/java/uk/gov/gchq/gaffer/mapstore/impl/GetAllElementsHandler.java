/*
 * Copyright 2017-2018 Crown Copyright
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
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewUtil;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

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
            Stream<Element> elements = mapImpl.getAllElements(getAllElements.getView().getGroups());
            elements = GetElementsUtil.applyDirectedTypeFilter(elements, getAllElements.getView().hasEdges(), getAllElements.getDirectedType());
            elements = GetElementsUtil.applyView(elements, schema, getAllElements.getView());
            elements = elements.map(element -> mapImpl.cloneElement(element, schema));
            elements = elements.map(element -> {
                ViewUtil.removeProperties(getAllElements.getView(), element);
                return element;
            });
            return new WrappedCloseableIterator<>(elements.iterator());
        }
    }
}
