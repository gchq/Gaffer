/*
 * Copyright 2017-2024 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.iterable.EmptyIterable;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Objects.isNull;

/**
 * An {@link OutputOperationHandler} for the {@link GetAdjacentIds} operation on the {@link MapStore}.
 */
public class GetAdjacentIdsHandler implements OutputOperationHandler<GetAdjacentIds, Iterable<? extends EntityId>> {

    @Override
    public Iterable<? extends EntityId> doOperation(final GetAdjacentIds operation,
                                                    final Context context,
                                                    final Store store)
            throws OperationException {
        return doOperation(operation, context, (MapStore) store);
    }

    private Iterable<EntityId> doOperation(final GetAdjacentIds operation,
                                           final Context context,
                                           final MapStore mapStore) {
        if (isNull(operation.getInput()) || !operation.getInput().iterator().hasNext()) {
            return new EmptyIterable<>();
        }
        return new EntityIdIterable(mapStore.getMapImpl(), operation, mapStore, context.getUser());
    }

    private static class EntityIdIterable implements Iterable<EntityId> {
        private final MapImpl mapImpl;
        private final GetAdjacentIds getAdjacentIds;
        private final Schema schema;
        private final User user;
        private final boolean supportsVisibility;

        EntityIdIterable(final MapImpl mapImpl, final GetAdjacentIds getAdjacentIds, final MapStore mapStore, final User user) {
            this.mapImpl = mapImpl;
            this.getAdjacentIds = getAdjacentIds;
            this.schema = mapStore.getSchema();
            this.user = user;
            this.supportsVisibility = mapStore.getTraits().contains(StoreTrait.VISIBILITY);
        }

        @Override
        public Iterator<EntityId> iterator() {
            // For each EntityId, get relevant edges with group-by properties
            // Create full Element
            // Apply view
            // Extract adjacent vertices
            Stream<Element> elementStream = Streams.toStream(getAdjacentIds.getInput())
                    .flatMap(entityId -> GetElementsUtil.getRelevantElements(mapImpl, entityId, getAdjacentIds.getView(), getAdjacentIds.getDirectedType(), getAdjacentIds.getIncludeIncomingOutGoing())
                            .stream()
                            .map(mapImpl::getAggElement));

            // Apply visibility
            if (this.supportsVisibility) {
                elementStream = GetElementsUtil.applyVisibilityFilter(elementStream, schema, user);
            }

            elementStream = elementStream.map(element -> mapImpl.cloneElement(element, schema));

            // Apply the view
            elementStream = GetElementsUtil.applyView(elementStream, schema, getAdjacentIds.getView(), true);

            final Stream<EntityId> adjacentIdsStream = elementStream
                    .filter(Objects::nonNull)
                    .map(element -> {
                        final Object nextVertex;
                        if (EdgeId.MatchedVertex.DESTINATION == ((EdgeId) element).getMatchedVertex()) {
                            nextVertex = ((EdgeId) element).getSource();
                        } else {
                            nextVertex = ((EdgeId) element).getDestination();
                        }

                        return new EntitySeed(nextVertex);
                    });

            return adjacentIdsStream.iterator();
        }
    }
}
