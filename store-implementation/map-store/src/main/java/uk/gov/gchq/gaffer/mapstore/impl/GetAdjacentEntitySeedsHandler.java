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
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.utils.Pair;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * An {@link OperationHandler} for the {@link GetAdjacentEntitySeeds} operation on the {@link MapStore}.
 */
public class GetAdjacentEntitySeedsHandler implements
        OperationHandler<GetAdjacentEntitySeeds, CloseableIterable<EntitySeed>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetAdjacentEntitySeedsHandler.class);

    @Override
    public CloseableIterable<EntitySeed> doOperation(final GetAdjacentEntitySeeds operation,
                                                     final Context context,
                                                     final Store store) throws OperationException {
        return doOperation(operation, (MapStore) store);
    }

    private CloseableIterable<EntitySeed> doOperation(final GetAdjacentEntitySeeds operation,
                                                      final MapStore mapStore) throws OperationException {
        if (null == operation.getSeeds() || !operation.getSeeds().iterator().hasNext()) {
            return new EmptyClosableIterable<>();
        }
        return new EntitySeedIterable(mapStore.getMapImpl(), operation);
    }

    private static class EntitySeedIterable extends WrappedCloseableIterable<EntitySeed> {
        private final MapImpl mapImpl;
        private final GetAdjacentEntitySeeds getAdjacentEntitySeeds;

        EntitySeedIterable(final MapImpl mapImpl, final GetAdjacentEntitySeeds getAdjacentEntitySeeds) {
            this.mapImpl = mapImpl;
            this.getAdjacentEntitySeeds = getAdjacentEntitySeeds;
        }

        @Override
        public CloseableIterator<EntitySeed> iterator() {
            // Create GetElements operation to be used to find relevant Elements to each EntitySeed. Do not add view
            // at this stage as that needs to be done later, after the full properties have been created.
            final GetElements<ElementSeed, Element> getElements = new GetElements.Builder<>()
                    .inOutType(getAdjacentEntitySeeds.getIncludeIncomingOutGoing())
                    .build();
            // For each EntitySeed, get relevant elements with group-by properties
            // Ignore Entities
            // Create full Element
            // Apply view
            // Extract adjacent nodes
            Stream<EntitySeed> entitySeedStream = StreamSupport.stream(getAdjacentEntitySeeds.getSeeds().spliterator(), true);
            Stream<Pair<EntitySeed, Set<Element>>> entitySeedRelevantElementsStream = entitySeedStream
                    .map(entitySeed -> {
                        final Set<Element> elements = GetElementsHandler
                                .getRelevantElements(mapImpl, entitySeed, getElements);
                        elements.removeIf(e -> e instanceof Entity);
                        return new Pair<>(entitySeed, elements);
                    })
                    .filter(pair -> 0 != pair.getSecond().size());
            Stream<Pair<EntitySeed, Set<Element>>> entitySeedRelevantFullElementsStream = entitySeedRelevantElementsStream
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

            Stream<Pair<EntitySeed, Stream<Element>>> entitySeedRelevantFullElementsStreamAfterView =
                    entitySeedRelevantFullElementsStream
                            .map(pair -> {
                                final Stream<Element> elementsAfterView = GetElementsHandler
                                        .applyView(pair.getSecond().stream(), mapImpl.schema,
                                                getAdjacentEntitySeeds.getView());
                                return new Pair<>(pair.getFirst(), elementsAfterView);
                            });

            Stream<EntitySeed> adjacentSeedsStream = entitySeedRelevantFullElementsStreamAfterView
                    .map(pair -> {
                        final Object seed = pair.getFirst().getVertex();
                        final Stream<EntitySeed> adjacentSeeds = pair.getSecond().map(element -> {
                            final Edge edge = (Edge) element;
                            final Object source = edge.getSource();
                            final Object destination = edge.getDestination();
                            if (source.equals(seed) && !destination.equals(seed)) {
                                return new EntitySeed(destination);
                            } else if (!source.equals(seed) && destination.equals(seed)) {
                                return new EntitySeed(source);
                            } else if (source.equals(seed) && destination.equals(seed)) {
                                return new EntitySeed(seed);
                            } else {
                                LOGGER.error("Found edge which doesn't correspond to the EntitySeed (edge = " +
                                        edge + "; seed = " + seed);
                                return null;
                            }
                        });
                        return adjacentSeeds;
                    })
                    .flatMap(Function.identity())
                    .filter(entitySeed -> null != entitySeed);

            return new WrappedCloseableIterator<>(adjacentSeedsStream.iterator());
        }
    }
}
