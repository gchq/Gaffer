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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterator;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * An {@link OutputOperationHandler} for the {@link GetAdjacentIds} operation on the {@link MapStore}.
 */
public class GetAdjacentIdsHandler implements
        OutputOperationHandler<GetAdjacentIds, CloseableIterable<? extends EntityId>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetAdjacentIds.class);

    @Override
    public CloseableIterable<? extends EntityId> doOperation(final GetAdjacentIds operation,
                                                             final Context context,
                                                             final Store store) throws OperationException {
        return doOperation(operation, (MapStore) store);
    }

    private CloseableIterable<EntityId> doOperation(final GetAdjacentIds operation,
                                                    final MapStore mapStore) throws OperationException {
        if (null == operation.getInput() || !operation.getInput().iterator().hasNext()) {
            return new EmptyClosableIterable<>();
        }
        return new EntityIdIterable(mapStore.getMapImpl(), operation);
    }

    private static class EntityIdIterable extends WrappedCloseableIterable<EntityId> {
        private final MapImpl mapImpl;
        private final GetAdjacentIds getAdjacentIds;

        EntityIdIterable(final MapImpl mapImpl, final GetAdjacentIds getAdjacentIds) {
            this.mapImpl = mapImpl;
            this.getAdjacentIds = getAdjacentIds;
        }

        @Override
        public CloseableIterator<EntityId> iterator() {
            // Create GetElements operation to be used to find relevant Elements to each EntityId. Do not add view
            // at this stage as that needs to be done later, after the full properties have been created.
            final GetElements getElements = new GetElements.Builder()
                    .inOutType(getAdjacentIds.getIncludeIncomingOutGoing())
                    .build();
            // For each EntityId, get relevant elements with group-by properties
            // Ignore Entities
            // Create full Element
            // Apply view
            // Extract adjacent nodes
            Stream<? extends EntityId> entityIdStream = Streams.toParallelStream(getAdjacentIds.getInput());
            Stream<? extends Pair<? extends EntityId, Set<Element>>> entityIdRelevantElementsStream = entityIdStream
                    .map(entityId -> {
                        final Set<Element> elements = GetElementsHandler.getRelevantElements(mapImpl, entityId, getElements);
                        elements.removeIf(e -> e instanceof Entity || !getAdjacentIds.validateFlags((Edge) e));
                        return new Pair<>(entityId, elements);
                    })
                    .filter(pair -> !pair.getSecond().isEmpty());
            Stream<Pair<EntityId, Set<Element>>> entityIdRelevantFullElementsStream = entityIdRelevantElementsStream
                    .map(pair -> {
                        final Set<Element> elementsWithProperties = new HashSet<>();
                        pair.getSecond()
                                .stream()
                                .map(element -> {
                                    final Element clone = element.emptyClone();
                                    clone.copyProperties(element.getProperties());
                                    return clone;
                                })
                                .map(element -> {
                                    final Properties properties = mapImpl.elementToProperties.get(element);
                                    element.copyProperties(properties);
                                    return element;
                                })
                                .forEach(elementsWithProperties::add);
                        return new Pair<>(pair.getFirst(), elementsWithProperties);
                    });

            Stream<Pair<EntityId, Stream<Element>>> entityIdRelevantFullElementsStreamAfterView =
                    entityIdRelevantFullElementsStream
                            .map(pair -> {
                                final Stream<Element> elementsAfterView = GetElementsHandler
                                        .applyView(pair.getSecond().stream(), mapImpl.schema,
                                                getAdjacentIds.getView());
                                return new Pair<>(pair.getFirst(), elementsAfterView);
                            });

            Stream<EntityId> adjacentIdsStream = entityIdRelevantFullElementsStreamAfterView
                    .map(pair -> {
                        final Object id = pair.getFirst().getVertex();
                        return pair.getSecond().<EntityId>map(element -> {
                            final Edge edge = (Edge) element;
                            final Object source = edge.getSource();
                            final Object destination = edge.getDestination();
                            if (source.equals(id) && !destination.equals(id)) {
                                return new EntitySeed(destination);
                            } else if (!source.equals(id) && destination.equals(id)) {
                                return new EntitySeed(source);
                            } else if (source.equals(id) && destination.equals(id)) {
                                return new EntitySeed(id);
                            } else {
                                LOGGER.error("Found edge which doesn't correspond to the EntityId (edge = {}; id = {}", edge, id);
                                return null;
                            }
                        });
                    })
                    .flatMap(Function.identity())
                    .filter(Objects::nonNull);

            return new WrappedCloseableIterator<>(adjacentIdsStream.iterator());
        }
    }
}
