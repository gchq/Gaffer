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
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterator;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.utils.ElementCloner;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static uk.gov.gchq.gaffer.mapstore.impl.MapImpl.COUNT;

/**
 * An {@link OperationHandler} for the {@link GetElements} operation on the {@link MapStore}.
 */
public class GetElementsHandler
        implements OperationHandler<GetElements<ElementSeed, Element>, CloseableIterable<Element>> {

    @Override
    public CloseableIterable<Element> doOperation(final GetElements<ElementSeed, Element> operation,
                                                  final Context context,
                                                  final Store store) throws OperationException {
        return doOperation(operation, (MapStore) store);
    }

    private CloseableIterable<Element> doOperation(final GetElements<ElementSeed, Element> operation,
                                                   final MapStore mapStore) throws OperationException {
        final MapImpl mapImpl = mapStore.getMapImpl();
        if (!mapImpl.maintainIndex) {
            throw new OperationException("Cannot execute getElements if the properties request that an index is not created");
        }
        final CloseableIterable<ElementSeed> seeds = operation.getSeeds();
        if (null == seeds || !seeds.iterator().hasNext()) {
            return new EmptyClosableIterable<>();
        }
        return new ElementsIterable(mapImpl, operation);
    }

    private static class ElementsIterable extends WrappedCloseableIterable<Element> {
        private final MapImpl mapImpl;
        private final GetElements<ElementSeed, Element> getElements;

        ElementsIterable(final MapImpl mapImpl, final GetElements<ElementSeed, Element> getElements) {
            this.mapImpl = mapImpl;
            this.getElements = getElements;
        }

        @Override
        public CloseableIterator<Element> iterator() {
            final Stream<Set<Element>> elementsSets = StreamSupport.stream(getElements.getSeeds().spliterator(), true)
                    .map(elementSeed -> getRelevantElements(mapImpl, elementSeed, getElements));
            final Stream<Element> elements = elementsSets.flatMap(s -> s.stream());
            final Stream<Element> elementsAfterIncludeEntitiesEdgesOption =
                    applyIncludeEntitiesEdgesOptions(elements, getElements.isIncludeEntities(), getElements.getIncludeEdges());
            // Generate final elements by copying properties into element
            Stream<Element> elementsWithProperties = elementsAfterIncludeEntitiesEdgesOption
                    .map(element -> {
                        if (mapImpl.groupsWithNoAggregation.contains(element.getGroup())) {
                            final int count = (int) mapImpl.elementToProperties.get(element).get(COUNT);
                            List<Element> duplicateElements = new ArrayList<>(count);
                            IntStream.range(0, count).forEach(i -> duplicateElements.add(element));
                            return duplicateElements;
                        } else {
                            final Properties properties = mapImpl.elementToProperties.get(element);
                            element.copyProperties(properties);
                            return Collections.singletonList(element);
                        }
                    })
                    .flatMap(x -> x.stream());
            final Stream<Element> afterView = applyView(elementsWithProperties, mapImpl.schema, getElements.getView());
            final Stream<Element> clonedElements = afterView.map(element -> ElementCloner.cloneElement(element, mapImpl.schema));
            if (!getElements.isPopulateProperties()) {
                // If populateProperties option is false then remove all properties
                return new WrappedCloseableIterator<>(clonedElements.map(e -> e.emptyClone()).iterator());
            }
            return new WrappedCloseableIterator<>(clonedElements.iterator());
        }
    }

    static Set<Element> getRelevantElements(final MapImpl mapImpl,
                                            final ElementSeed elementSeed,
                                            final GetElements<ElementSeed, Element> getElements) {
        if (elementSeed instanceof EntitySeed) {
            final Set<Element> relevantElements = new HashSet<>();
            if (null != mapImpl.entitySeedToElements.get(elementSeed)) {
                relevantElements.addAll(mapImpl.entitySeedToElements.get(elementSeed));
            }
            if (relevantElements.isEmpty()) {
                return Collections.emptySet();
            }
            // Apply inOutType options
            // If option is BOTH then nothing to do
            if (getElements.getIncludeIncomingOutGoing() == GetOperation.IncludeIncomingOutgoingType.INCOMING) {
                relevantElements.removeIf(e -> e instanceof Edge
                        && ((Edge) e).isDirected()
                        && ((Edge) e).getSource().equals(((EntitySeed) elementSeed).getVertex()));
            }
            if (getElements.getIncludeIncomingOutGoing() == GetOperation.IncludeIncomingOutgoingType.OUTGOING) {
                relevantElements.removeIf(e -> e instanceof Edge
                        && ((Edge) e).isDirected()
                        && ((Edge) e).getDestination().equals(((EntitySeed) elementSeed).getVertex()));
            }
            // Apply seedMatching option
            // If option is RELATED then nothing to do
            if (getElements.getSeedMatching() == GetOperation.SeedMatchingType.EQUAL) {
                relevantElements.removeIf(e -> e instanceof Edge);
            }
            return relevantElements;
        } else {
            final EdgeSeed edgeSeed = (EdgeSeed) elementSeed;
            final Set<Element> relevantElements = new HashSet<>();
            if (mapImpl.edgeSeedToElements.get(edgeSeed) != null) {
                relevantElements.addAll(mapImpl.edgeSeedToElements.get(edgeSeed));
            }
            if (mapImpl.entitySeedToElements.get(new EntitySeed(edgeSeed.getSource())) != null) {
                final Set<Element> related = new HashSet<>();
                related.addAll(mapImpl.entitySeedToElements.get(new EntitySeed(edgeSeed.getSource())));
                related.removeIf(e -> e instanceof Edge);
                relevantElements.addAll(related);
            }
            if (mapImpl.entitySeedToElements.get(new EntitySeed(edgeSeed.getDestination())) != null) {
                final Set<Element> related = new HashSet<>();
                related.addAll(mapImpl.entitySeedToElements.get(new EntitySeed(edgeSeed.getDestination())));
                related.removeIf(e -> e instanceof Edge);
                relevantElements.addAll(related);
            }
            // Apply seedMatching option
            // If option is RELATED then nothing to do
            if (getElements.getSeedMatching() == GetOperation.SeedMatchingType.EQUAL) {
                relevantElements.removeIf(e -> e instanceof Entity);
            }
            return relevantElements;
        }
    }

    static Stream<Element> applyIncludeEntitiesEdgesOptions(final Stream<Element> elements,
                                                     final boolean includeEntities,
                                                     final GetOperation.IncludeEdgeType includeEdgeType) {
        // Apply include entities option
        final Stream<Element> elementsAfterIncludeEntitiesOption;
        if (!includeEntities) {
            elementsAfterIncludeEntitiesOption = elements.filter(e -> e instanceof Edge);
        } else {
            elementsAfterIncludeEntitiesOption = elements;
        }
        // Apply include edges option
        Stream<Element> elementsAfterIncludeEdgesOption = elementsAfterIncludeEntitiesOption;
        if (includeEdgeType == GetOperation.IncludeEdgeType.NONE) {
            elementsAfterIncludeEdgesOption = elementsAfterIncludeEntitiesOption.filter(e -> !(e instanceof Edge));
        } else if (includeEdgeType == GetOperation.IncludeEdgeType.ALL) {
            elementsAfterIncludeEdgesOption = elementsAfterIncludeEntitiesOption;
        } else if (includeEdgeType == GetOperation.IncludeEdgeType.DIRECTED) {
            elementsAfterIncludeEdgesOption = elementsAfterIncludeEntitiesOption.filter(e -> {
                if (e instanceof Entity) {
                    return true;
                }
                final Edge edge = (Edge) e;
                return edge.isDirected();
            });
        } else if (includeEdgeType == GetOperation.IncludeEdgeType.UNDIRECTED) {
            elementsAfterIncludeEdgesOption = elementsAfterIncludeEntitiesOption.filter(e -> {
                if (e instanceof Entity) {
                    return true;
                }
                final Edge edge = (Edge) e;
                return !edge.isDirected();
            });
        }
        return elementsAfterIncludeEdgesOption;
    }

    static Stream<Element> applyView(final Stream<Element> elementStream, final Schema schema, final View view) {
        final Set<String> viewGroups = view.getGroups();
        Stream<Element> stream = elementStream;
        // Check group is valid
        if (!view.getEntityGroups().equals(schema.getEntityGroups())
                || !view.getEdgeGroups().equals(schema.getEdgeGroups())) {
            stream = stream.filter(e -> viewGroups.contains(e.getGroup()));
        }

        // Apply pre-aggregation filter
        stream = stream.filter(e -> {
            final ViewElementDefinition ved = view.getElement(e.getGroup());
            return ved.getPreAggregationFilter() == null || ved.getPreAggregationFilter().filter(e);
        });

        // Apply post-aggregation filter
        stream = stream.filter(e -> {
            final ViewElementDefinition ved = view.getElement(e.getGroup());
            return ved.getPostAggregationFilter() == null || ved.getPostAggregationFilter().filter(e);
        });

        // Apply transform
        stream = stream.map(e -> {
            final ViewElementDefinition ved = view.getElement(e.getGroup());
            final ElementTransformer transformer = ved.getTransformer();
            if (transformer != null) {
                transformer.transform(e);
            }
            return e;
        });

        // Apply post transform filter
        stream = stream.filter(e -> {
            final ViewElementDefinition ved = view.getElement(e.getGroup());
            return ved.getPostTransformFilter() == null || ved.getPostTransformFilter().filter(e);
        });

        return stream;
    }
}
