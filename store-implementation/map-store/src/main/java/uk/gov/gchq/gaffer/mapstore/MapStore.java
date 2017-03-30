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
package uk.gov.gchq.gaffer.mapstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.mapstore.operation.handler.AddElementsHandler;
import uk.gov.gchq.gaffer.mapstore.operation.handler.GetAdjacentEntitySeedsOperationHandler;
import uk.gov.gchq.gaffer.mapstore.operation.handler.GetAllElementsHandler;
import uk.gov.gchq.gaffer.mapstore.operation.handler.GetElementsOperationHandler;
import uk.gov.gchq.gaffer.mapstore.utils.Pair;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * An implementation of {@link Store} that uses any class that implements Java's {@link Map} interface to store the
 * {@link Element}s. The {@link Element} objects are stored in memory, i.e. no serialisation is performed.
 *
 * <p>It is designed to support efficient aggregation of properties. The key of the {@link Map} is the {@link Element}
 * with any group-by properties, and the value is the non-group-by properties. This allows very quick aggregation of
 * properties from a new {@link Element} with existing properties.
 *
 * <p>Indices can optionally be maintained to allow quick look-up of {@link Element}s based on {@link EntitySeed}s
 * or {@link EdgeSeed}s.
 */
public class MapStore extends Store {
    private static final Logger LOGGER = LoggerFactory.getLogger(MapStore.class);
    private static final Set<StoreTrait> TRAITS = new HashSet<>(Arrays.asList(
            StoreTrait.STORE_AGGREGATION,
            StoreTrait.PRE_AGGREGATION_FILTERING,
            StoreTrait.POST_AGGREGATION_FILTERING,
            StoreTrait.TRANSFORMATION,
            StoreTrait.POST_TRANSFORMATION_FILTERING));
    private static final String COUNT = "COUNT";

    // elementToProperties maps from an Element containing the group-by properties to a Properties object without the
    // group-by properties
    private Map<Element, Properties> elementToProperties;
    // entitySeedToElements is a map from an EntitySeed to the element key from elementToProperties
    private Map<EntitySeed, Set<Element>> entitySeedToElements;
    // edgeSeedToElements is a map from an EdgeSeed to the element key from elementToProperties
    private Map<EdgeSeed, Set<Element>> edgeSeedToElements;
    private boolean maintainEntitySeedIndex;
    private boolean maintainEdgeSeedIndex;
    private final Map<String, Set<String>> groupToGroupByProperties = new HashMap<>();
    private final Map<String, Set<String>> groupToNonGroupByProperties = new HashMap<>();
    private final Set<String> groupsWithNoAggregation = new HashSet<>();

    @Override
    public void initialise(final Schema schema, final StoreProperties storeProperties) throws StoreException {
        if (!(storeProperties instanceof MapStoreProperties)) {
            throw new StoreException("storeProperties must be an instance of MapStoreProperties");
        }
        // Initialise store
        final MapStoreProperties mapStoreProperties = (MapStoreProperties) storeProperties;
        super.initialise(schema, mapStoreProperties);
        maintainEntitySeedIndex = mapStoreProperties.getCreateEntitySeedIndex();
        maintainEdgeSeedIndex = mapStoreProperties.getCreateEdgeSeedIndex();
        // Initialise maps
        try {
            elementToProperties = Class.forName(mapStoreProperties.getMapClass()).asSubclass(Map.class).newInstance();
            if (maintainEntitySeedIndex) {
                entitySeedToElements = Class.forName(mapStoreProperties.getMapClass()).asSubclass(Map.class).newInstance();
            }
            if (maintainEdgeSeedIndex) {
                edgeSeedToElements = Class.forName(mapStoreProperties.getMapClass()).asSubclass(Map.class).newInstance();
            }
        } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new StoreException("Exception instantiating map of class " + mapStoreProperties.getMapClass(), e);
        }
        // Work out which properties are group-by properties and which are non-group-by properties for each group
        schema.getEntityGroups().forEach(g -> addToGroupByMap(getSchema(), g));
        schema.getEdgeGroups().forEach(g -> addToGroupByMap(getSchema(), g));
    }

    private void addToGroupByMap(final Schema schema, final String group) {
        final SchemaElementDefinition sed = schema.getElement(group);
        groupToGroupByProperties.put(group, sed.getGroupBy());
        if (null == sed.getGroupBy() || sed.getGroupBy().size() == 0) {
            groupsWithNoAggregation.add(group);
        }
        final Set<String> nonGroupByProperties = new HashSet<>(sed.getProperties());
        nonGroupByProperties.removeAll(sed.getGroupBy());
        groupToNonGroupByProperties.put(group, nonGroupByProperties);
    }

    @Override
    public Set<StoreTrait> getTraits() {
        return TRAITS;
    }

    @Override
    public boolean isValidationRequired() {
        return false;
    }

    @Override
    protected void addAdditionalOperationHandlers() {
    }

    @Override
    protected OperationHandler<GetElements<ElementSeed, Element>, CloseableIterable<Element>> getGetElementsHandler() {
        return new GetElementsOperationHandler();
    }

    @Override
    protected OperationHandler<GetAllElements<Element>, CloseableIterable<Element>> getGetAllElementsHandler() {
        return new GetAllElementsHandler();
    }

    @Override
    protected OperationHandler<? extends GetAdjacentEntitySeeds, CloseableIterable<EntitySeed>> getAdjacentEntitySeedsHandler() {
        return new GetAdjacentEntitySeedsOperationHandler();
    }

    @Override
    protected OperationHandler<? extends AddElements, Void> getAddElementsHandler() {
        return new AddElementsHandler();
    }

    @Override
    protected <OUTPUT> OUTPUT doUnhandledOperation(final Operation<?, OUTPUT> operation, final Context context) {
        throw new UnsupportedOperationException("Operation " + operation.getClass() + " is not supported");
    }

    public void addElements(final CloseableIterable<Element> elements) {
        StreamSupport.stream(elements.spliterator(), true)
                .forEach(element -> {
                    // Update main map of element with group-by properties to properties
                    final Element elementWithGroupByProperties = updateElementToProperties(element);
                    // Update entitySeedToElements if index required
                    if (maintainEntitySeedIndex) {
                        updateEntitySeedIndex(elementWithGroupByProperties);
                    }
                    // Update edgeSeedToElements if index required
                    if (maintainEdgeSeedIndex) {
                        updateEdgeSeedIndex(elementWithGroupByProperties);
                    }
                });
    }

    private Element updateElementToProperties(final Element element) {
        final Element elementForIndexing;
        if (groupsWithNoAggregation.contains(element.getGroup())) {
            elementForIndexing = updateElementToPropertiesNoGroupBy(element);
        } else {
            elementForIndexing = updateElementToPropertiesWithGroupBy(element);
        }
        return elementForIndexing;
    }

    private Element updateElementToPropertiesWithGroupBy(final Element element) {
        final String group = element.getGroup();
        final Element elementWithGroupByProperties = element.emptyClone();
        final Properties properties = new Properties();
        groupToGroupByProperties.get(group)
                .forEach(propertyName -> elementWithGroupByProperties
                        .putProperty(propertyName, element.getProperty(propertyName)));
        groupToNonGroupByProperties.get(group)
                .forEach(propertyName -> properties.put(propertyName, element.getProperty(propertyName)));
        if (!elementToProperties.containsKey(elementWithGroupByProperties)) {
            elementToProperties.put(elementWithGroupByProperties, new Properties());
        }
        final Properties existingProperties = elementToProperties.get(elementWithGroupByProperties);
        final ElementAggregator aggregator = getSchema().getElement(group).getAggregator();
        aggregator.initFunctions();
        aggregator.aggregate(existingProperties);
        aggregator.aggregate(properties);
        final Properties aggregatedProperties = new Properties();
        aggregator.state(aggregatedProperties);
        elementToProperties.put(elementWithGroupByProperties, aggregatedProperties);
        return elementWithGroupByProperties;
    }

    private Element updateElementToPropertiesNoGroupBy(final Element element) {
        final Properties existingProperties = elementToProperties.get(element);
        if (null == existingProperties) {
            // Clone element and add to map with properties containing 1
            final Element elementWithGroupByProperties = element.emptyClone();
            elementWithGroupByProperties.copyProperties(element.getProperties());
            final Properties properties = new Properties();
            properties.put(COUNT, 1);
            elementToProperties.put(elementWithGroupByProperties, properties);
        } else {
            existingProperties.put(COUNT, ((int) existingProperties.get(COUNT)) + 1);
        }
        return element;
    }

    private void updateEntitySeedIndex(final Element elementWithGroupByProperties) {
        if (elementWithGroupByProperties instanceof Entity) {
            final EntitySeed entitySeed = new EntitySeed(((Entity) elementWithGroupByProperties).getVertex());
            updateEntitySeedToElementsMap(entitySeed, elementWithGroupByProperties);
        } else {
            final Edge edge = (Edge) elementWithGroupByProperties;
            final EntitySeed sourceEntitySeed = new EntitySeed(edge.getSource());
            final EntitySeed destinationEntitySeed = new EntitySeed(edge.getDestination());
            updateEntitySeedToElementsMap(sourceEntitySeed, elementWithGroupByProperties);
            updateEntitySeedToElementsMap(destinationEntitySeed, elementWithGroupByProperties);
        }
    }

    private void updateEdgeSeedIndex(final Element elementWithGroupByProperties) {
        if (elementWithGroupByProperties instanceof Edge) {
            final Edge edge = (Edge) elementWithGroupByProperties;
            final EdgeSeed edgeSeed = new EdgeSeed(edge.getSource(), edge.getDestination(), edge.isDirected());
            updateEdgeSeedToElementsMap(edgeSeed, elementWithGroupByProperties);
        }
    }

    private void updateEntitySeedToElementsMap(final EntitySeed entitySeed, final Element element) {
        if (!entitySeedToElements.containsKey(entitySeed)) {
            entitySeedToElements.put(entitySeed, new HashSet<>());
        }
        entitySeedToElements.get(entitySeed).add(element);
    }

    private void updateEdgeSeedToElementsMap(final EdgeSeed edgeSeed, final Element element) {
        if (!edgeSeedToElements.containsKey(edgeSeed)) {
            edgeSeedToElements.put(edgeSeed, new HashSet<>());
        }
        edgeSeedToElements.get(edgeSeed).add(element);
    }

    public CloseableIterable<Element> getAllElements(final GetAllElements<Element> getAllElements) {
        // NB Create the stream inside the iterator method to ensure that the resulting iterable can be looped
        // through multiple times.
        return new WrappedCloseableIterable<>(new Iterable<Element>() {

            @Override
            public Iterator<Element> iterator() {
                // Create stream of elements from elementToProperties by copying the properties from the value into the key
                Stream<Element> elements = elementToProperties.entrySet()
                        .stream()
                        .map(x -> {
                            final Element element = x.getKey();
                            final Properties properties = x.getValue();
                            if (groupsWithNoAggregation.contains(x.getKey().getGroup())) {
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
                final Stream<Element> elementsAfterIncludeEntitiesEdgesOption =
                        applyIncludeEntitiesEdgesOptions(elements, getAllElements.isIncludeEntities(), getAllElements.getIncludeEdges());
                final Stream<Element> afterView = applyView(elementsAfterIncludeEntitiesEdgesOption, getAllElements.getView());
                if (!getAllElements.isPopulateProperties()) {
                    // If populateProperties option is false then remove all properties
                    return afterView.map(e -> e.emptyClone()).iterator();
                }
                return afterView.iterator();
            }
        });
    }

    public CloseableIterable<Element> getElements(final GetElements<ElementSeed, Element> getElements) throws OperationException {
        if (!maintainEntitySeedIndex || !maintainEdgeSeedIndex) {
            throw new OperationException("Cannot execute getElements if indices are not set");
        }
        final CloseableIterable<ElementSeed> seeds = getElements.getSeeds();
        if (null == seeds || !seeds.iterator().hasNext()) {
            return new EmptyClosableIterable<>();
        }
        final View view = getElements.getView();
        return new WrappedCloseableIterable<>(new Iterable<Element>() {
            @Override
            public Iterator<Element> iterator() {
                final Stream<Set<Element>> elementsSets = StreamSupport.stream(seeds.spliterator(), true)
                        .map(elementSeed -> getRelevantElements(elementSeed, getElements));
                final Stream<Element> elements = elementsSets.flatMap(s -> s.stream());
                final Stream<Element> elementsAfterIncludeEntitiesEdgesOption =
                        applyIncludeEntitiesEdgesOptions(elements, getElements.isIncludeEntities(), getElements.getIncludeEdges());
                // Generate final elements by copying properties into element
                Stream<Element> elementsWithProperties = elementsAfterIncludeEntitiesEdgesOption
                        .map(element -> {
                            if (groupsWithNoAggregation.contains(element.getGroup())) {
                                final int count = (int) elementToProperties.get(element).get(COUNT);
                                List<Element> duplicateElements = new ArrayList<>(count);
                                IntStream.range(0, count).forEach(i -> duplicateElements.add(element));
                                return duplicateElements;
                            } else {
                                final Properties properties = elementToProperties.get(element);
                                element.copyProperties(properties);
                                return Collections.singletonList(element);
                            }
                        })
                        .flatMap(x -> x.stream());
                final Stream<Element> afterView = applyView(elementsWithProperties, view);
                if (!getElements.isPopulateProperties()) {
                    // If populateProperties option is false then remove all properties
                    return afterView.map(e -> e.emptyClone()).iterator();
                }
                return afterView.iterator();

            }
        });
    }

    private Stream<Element> applyIncludeEntitiesEdgesOptions(final Stream<Element> elements,
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

    private Set<Element> getRelevantElements(final ElementSeed elementSeed,
                                             final GetElements<ElementSeed, Element> getElements) {
        if (elementSeed instanceof EntitySeed) {
            final Set<Element> relevantElements = new HashSet<>();
            if (null != entitySeedToElements.get(elementSeed)) {
                relevantElements.addAll(entitySeedToElements.get(elementSeed));
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
            if (edgeSeedToElements.get(edgeSeed) != null) {
                relevantElements.addAll(edgeSeedToElements.get(edgeSeed));
            }
            if (entitySeedToElements.get(new EntitySeed(edgeSeed.getSource())) != null) {
                final Set<Element> related = new HashSet<>();
                related.addAll(entitySeedToElements.get(new EntitySeed(edgeSeed.getSource())));
                related.removeIf(e -> e instanceof Edge);
                relevantElements.addAll(related);
            }
            if (entitySeedToElements.get(new EntitySeed(edgeSeed.getDestination())) != null) {
                final Set<Element> related = new HashSet<>();
                related.addAll(entitySeedToElements.get(new EntitySeed(edgeSeed.getDestination())));
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

    private Stream<Element> applyView(final Stream<Element> elementStream, final View view) {
        final Set<String> viewGroups = view.getGroups();
        Stream<Element> stream = elementStream;
        // Check group is valid
        if (!view.getEntityGroups().equals(getSchema().getEntityGroups())
                || !view.getEdgeGroups().equals(getSchema().getEdgeGroups())) {
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

    public CloseableIterable<EntitySeed> getAdjacentEntitySeeds(final GetAdjacentEntitySeeds getAdjacentEntitySeeds)
            throws OperationException {
        if (null == getAdjacentEntitySeeds.getSeeds() || !getAdjacentEntitySeeds.getSeeds().iterator().hasNext()) {
            return new EmptyClosableIterable<>();
        }
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
        return new WrappedCloseableIterable<EntitySeed>(new Iterable<EntitySeed>() {
            @Override
            public Iterator<EntitySeed> iterator() {
                Stream<EntitySeed> entitySeedStream = StreamSupport.stream(getAdjacentEntitySeeds.getSeeds().spliterator(), true);
                Stream<Pair<EntitySeed, Set<Element>>> entitySeedRelevantElementsStream = entitySeedStream
                        .map(entitySeed -> {
                            final Set<Element> elements = getRelevantElements(entitySeed, getElements);
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
                                        final Properties properties = elementToProperties.get(element);
                                        element.copyProperties(properties);
                                        return element;
                                    })
                                    .forEach(elementsWithProperties::add);
                            return new Pair<>(pair.getFirst(), elementsWithProperties);
                        });

                Stream<Pair<EntitySeed, Stream<Element>>> entitySeedRelevantFullElementsStreamAfterView =
                        entitySeedRelevantFullElementsStream
                                .map(pair -> {
                                    final Stream<Element> elementsAfterView = applyView(pair.getSecond().stream(),
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

                return adjacentSeedsStream.iterator();
            }
        });
    }

}
