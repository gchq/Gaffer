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
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.utils.ElementCloner;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.SeedMatching.SeedMatchingType;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters.DirectedType;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static uk.gov.gchq.gaffer.mapstore.impl.MapImpl.COUNT;

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
        if (!mapImpl.maintainIndex) {
            throw new OperationException("Cannot execute getElements if the properties request that an index is not created");
        }
        final Iterable<? extends ElementId> seeds = operation.getInput();
        if (null == seeds) {
            return new EmptyClosableIterable<>();
        }
        return new ElementsIterable(mapImpl, operation);
    }

    private static class ElementsIterable extends WrappedCloseableIterable<Element> {
        private final MapImpl mapImpl;
        private final GetElements getElements;

        ElementsIterable(final MapImpl mapImpl, final GetElements getElements) {
            this.mapImpl = mapImpl;
            this.getElements = getElements;
        }

        @Override
        public CloseableIterator<Element> iterator() {
            final Stream<Set<Element>> elementsSets = Streams.toParallelStream(getElements.getInput())
                    .map(elementId -> getRelevantElements(mapImpl, elementId, getElements));
            final Stream<Element> elements = elementsSets.flatMap(s -> s.stream());
            final Stream<Element> elementsAfterIncludeEntitiesEdgesOption =
                    applyIncludeEntitiesEdgesOptions(elements, getElements.getView().hasEntities(), getElements.getView().hasEdges(), getElements.getDirectedType());
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
            return new WrappedCloseableIterator<>(clonedElements.iterator());
        }
    }

    static Set<Element> getRelevantElements(final MapImpl mapImpl,
                                            final ElementId elementId,
                                            final GetElements getElements) {
        if (elementId instanceof EntityId) {
            final Set<Element> relevantElements = new HashSet<>();
            if (null != mapImpl.entityIdToElements.get(elementId)) {
                relevantElements.addAll(mapImpl.entityIdToElements.get(elementId));
            }
            if (relevantElements.isEmpty()) {
                return Collections.emptySet();
            }
            // Apply inOutType options
            // If option is BOTH then nothing to do
            if (getElements.getIncludeIncomingOutGoing() == IncludeIncomingOutgoingType.INCOMING) {
                relevantElements.removeIf(e -> e instanceof Edge
                        && ((Edge) e).isDirected()
                        && ((Edge) e).getSource().equals(((EntityId) elementId).getVertex()));
            }
            if (getElements.getIncludeIncomingOutGoing() == IncludeIncomingOutgoingType.OUTGOING) {
                relevantElements.removeIf(e -> e instanceof Edge
                        && ((Edge) e).isDirected()
                        && ((Edge) e).getDestination().equals(((EntityId) elementId).getVertex()));
            }
            // Apply seedMatching option
            // If option is RELATED then nothing to do
            if (getElements.getSeedMatching() == SeedMatchingType.EQUAL) {
                relevantElements.removeIf(e -> e instanceof Edge);
            }
            return relevantElements;
        } else {
            final EdgeId edgeId = (EdgeSeed) elementId;
            final Set<Element> relevantElements = new HashSet<>();
            if (mapImpl.edgeIdToElements.get(edgeId) != null) {
                relevantElements.addAll(mapImpl.edgeIdToElements.get(edgeId));
            }
            if (mapImpl.entityIdToElements.get(new EntitySeed(edgeId.getSource())) != null) {
                final Set<Element> related = new HashSet<>();
                related.addAll(mapImpl.entityIdToElements.get(new EntitySeed(edgeId.getSource())));
                related.removeIf(e -> e instanceof Edge);
                relevantElements.addAll(related);
            }
            if (mapImpl.entityIdToElements.get(new EntitySeed(edgeId.getDestination())) != null) {
                final Set<Element> related = new HashSet<>();
                related.addAll(mapImpl.entityIdToElements.get(new EntitySeed(edgeId.getDestination())));
                related.removeIf(e -> e instanceof Edge);
                relevantElements.addAll(related);
            }
            // Apply seedMatching option
            // If option is RELATED then nothing to do
            if (getElements.getSeedMatching() == SeedMatchingType.EQUAL) {
                relevantElements.removeIf(e -> e instanceof Entity);
            }
            return relevantElements;
        }
    }

    static Stream<Element> applyIncludeEntitiesEdgesOptions(final Stream<Element> elements,
                                                            final boolean includeEntities,
                                                            final boolean includeEdges,
                                                            final DirectedType directedType) {
        // Apply include entities option
        final Stream<Element> elementsAfterIncludeEntitiesOption;
        if (!includeEntities) {
            elementsAfterIncludeEntitiesOption = elements.filter(e -> e instanceof Edge);
        } else {
            elementsAfterIncludeEntitiesOption = elements;
        }
        // Apply include edges option
        Stream<Element> elementsAfterIncludeEdgesOption = elementsAfterIncludeEntitiesOption;
        if (!includeEdges) {
            elementsAfterIncludeEdgesOption = elementsAfterIncludeEntitiesOption.filter(e -> !(e instanceof Edge));
        } else if (null == directedType || directedType == DirectedType.BOTH) {
            elementsAfterIncludeEdgesOption = elementsAfterIncludeEntitiesOption;
        } else if (directedType == DirectedType.DIRECTED) {
            elementsAfterIncludeEdgesOption = elementsAfterIncludeEntitiesOption.filter(e -> {
                if (e instanceof Entity) {
                    return true;
                }
                final Edge edge = (Edge) e;
                return edge.isDirected();
            });
        } else if (directedType == DirectedType.UNDIRECTED) {
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
            return ved.getPreAggregationFilter() == null || ved.getPreAggregationFilter().test(e);
        });

        // Apply post-aggregation filter
        stream = stream.filter(e -> {
            final ViewElementDefinition ved = view.getElement(e.getGroup());
            return ved.getPostAggregationFilter() == null || ved.getPostAggregationFilter().test(e);
        });

        // Apply transform
        stream = stream.map(e -> {
            final ViewElementDefinition ved = view.getElement(e.getGroup());
            final ElementTransformer transformer = ved.getTransformer();
            if (transformer != null) {
                transformer.apply(e);
            }
            return e;
        });

        // Apply post transform filter
        stream = stream.filter(e -> {
            final ViewElementDefinition ved = view.getElement(e.getGroup());
            return ved.getPostTransformFilter() == null || ved.getPostTransformFilter().test(e);
        });

        return stream;
    }
}
