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
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterator;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewUtil;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.stream.Stream;

/**
 * An {@link OutputOperationHandler} for the {@link GetElements} operation on the {@link MapStore}.
 */
public class GetElementsHandler
        implements OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> {

    @Override
    public CloseableIterable<Element> doOperation(final GetElements operation,
                                                  final Context context,
                                                  final Store store) throws OperationException {
        return doOperation(operation, (MapStore) store);
    }

    private CloseableIterable<Element> doOperation(final GetElements operation,
                                                   final MapStore mapStore) throws OperationException {
        final MapImpl mapImpl = mapStore.getMapImpl();
        if (!mapImpl.isMaintainIndex()) {
            throw new OperationException("Cannot execute getElements if the properties request that an index is not created");
        }
        final Iterable<? extends ElementId> seeds = operation.getInput();
        if (null == seeds) {
            return new EmptyClosableIterable<>();
        }
        return new ElementsIterable(mapImpl, operation, mapStore.getSchema());
    }

    private static class ElementsIterable extends WrappedCloseableIterable<Element> {
        private final MapImpl mapImpl;
        private final GetElements getElements;
        private final Schema schema;

        ElementsIterable(final MapImpl mapImpl, final GetElements getElements, final Schema schema) {
            this.mapImpl = mapImpl;
            this.getElements = getElements;
            this.schema = schema;
        }

        @Override
        public CloseableIterator<Element> iterator() {
            Stream<Element> elements = Streams.toStream(getElements.getInput())
                    .flatMap(elementId -> GetElementsUtil.getRelevantElements(mapImpl, elementId, getElements.getView(), getElements.getDirectedType(), getElements.getIncludeIncomingOutGoing(), getElements.getSeedMatching()).stream());
            elements = elements.flatMap(e -> Streams.toStream(mapImpl.getElements(e)));
            elements = GetElementsUtil.applyView(elements, schema, getElements.getView());
            elements = elements.map(element -> mapImpl.cloneElement(element, schema));
            elements = elements.map(element -> {
                ViewUtil.removeProperties(getElements.getView(), element);
                return element;
            });
            return new WrappedCloseableIterator<>(elements.iterator());
        }
    }
}
