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
package uk.gov.gchq.gaffer.mapstore.impl;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterator;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewUtil;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.FieldDeclaration;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.stream.Stream;

/**
 * An {@link OperationHandler} for the GetElements operation on the {@link MapStore}.
 */
public class GetElementsHandler implements OperationHandler<CloseableIterable<? extends Element>> {

    public static final String KEY_VIEW = "view";
    public static final String KEY_INCLUDE_INCOMING_OUT_GOING = "includeIncomingOutGoing";
    public static final String KEY_DIRECTED_TYPE = "directedType";
    public static final String KEY_INPUT = "input";
    @Deprecated
    public static final String KEY_SEED_MATCHING = "seedMatching";

    @Override
    public FieldDeclaration getFieldDeclaration() {
        return new FieldDeclaration()
                .fieldOptional(KEY_VIEW, View.class)
                .fieldOptional(KEY_INCLUDE_INCOMING_OUT_GOING, SeededGraphFilters.IncludeIncomingOutgoingType.class)
                .fieldOptional(KEY_DIRECTED_TYPE, DirectedType.class)
                .fieldOptional(KEY_INPUT, Iterable.class)
                //TODO deprecate and remove.
                .fieldOptional(KEY_SEED_MATCHING, SeedMatching.SeedMatchingType.class);
    }

    @Override
    public CloseableIterable<Element> _doOperation(final Operation operation,
                                                   final Context context,
                                                   final Store store) throws OperationException {
        final MapImpl mapImpl = ((MapStore) store).getMapImpl();
        if (!mapImpl.isMaintainIndex()) {
            throw new OperationException("Cannot execute getElements if the properties request that an index is not created");
        }
        final Iterable<? extends ElementId> seeds = (Iterable<? extends ElementId>) operation.input();
        if (null == seeds) {
            return new EmptyClosableIterable<>();
        }
        return new ElementsIterable(mapImpl, operation, (MapStore) store, context.getUser());
    }

    private static class ElementsIterable extends WrappedCloseableIterable<Element> {
        private final MapImpl mapImpl;
        private final Operation getElements;
        private final Schema schema;
        private final User user;
        private final boolean supportsVisibility;

        ElementsIterable(final MapImpl mapImpl, final Operation getElements, final MapStore mapStore, final User user) {
            this.mapImpl = mapImpl;
            this.getElements = getElements;
            this.schema = mapStore.getSchema();
            this.user = user;
            this.supportsVisibility = mapStore.getTraits().contains(StoreTrait.VISIBILITY);

        }

        @Override
        public CloseableIterator<Element> iterator() {
            Stream<Element> elements = Streams.toStream((Iterable<? extends ElementId>) getElements.getInput())
                    .flatMap(elementId -> GetElementsUtil.getRelevantElements(mapImpl, elementId, (View) getElements.get(KEY_VIEW), (DirectedType) getElements.get(KEY_DIRECTED_TYPE), (SeededGraphFilters.IncludeIncomingOutgoingType) getElements.get(KEY_INCLUDE_INCOMING_OUT_GOING), (SeedMatching.SeedMatchingType) getElements.get(KEY_SEED_MATCHING)).stream())
                    .distinct();
            elements = elements.flatMap(e -> Streams.toStream(mapImpl.getElements(e)));
            if (this.supportsVisibility) {
                elements = GetElementsUtil.applyVisibilityFilter(elements, schema, user);
            }
            elements = elements.map(element -> mapImpl.cloneElement(element, schema));
            elements = GetElementsUtil.applyView(elements, schema, (View) getElements.get(KEY_VIEW));
            elements = elements.map(element -> {
                ViewUtil.removeProperties((View) getElements.get(KEY_VIEW), element);
                return element;
            });
            return new WrappedCloseableIterator<>(elements.iterator());
        }
    }
}

