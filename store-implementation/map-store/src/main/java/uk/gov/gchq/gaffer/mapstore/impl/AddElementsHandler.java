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
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

/**
 * An {@link OperationHandler} for the {@link AddElements} operation on the {@link MapStore}.
 */
public class AddElementsHandler implements OperationHandler<AddElements, Void> {

    @Override
    public Void doOperation(final AddElements addElements, final Context context, final Store store) throws OperationException {
        doOperation(addElements, (MapStore) store);
        return null;
    }

    private void doOperation(final AddElements addElements, final MapStore mapStore) {
        final MapImpl mapImpl = mapStore.getMapImpl();
        addElements(addElements.getElements(), mapImpl, mapStore.getSchema());
    }

    private void addElements(final CloseableIterable<Element> elements, final MapImpl mapImpl, final Schema schema) {
        final boolean maintainIndex = mapImpl.maintainIndex;
        final Set<String> groupsWithNoAggregation = mapImpl.groupsWithNoAggregation;
        final Map<EntitySeed, Set<Element>> entitySeedToElements = mapImpl.entitySeedToElements;
        final Map<EdgeSeed, Set<Element>> edgeSeedToElements = mapImpl.edgeSeedToElements;
        final Map<String, Set<String>> groupToGroupByProperties = mapImpl.groupToGroupByProperties;
        final Map<String, Set<String>> groupToNonGroupByProperties = mapImpl.groupToNonGroupByProperties;
        final Map<Element, Properties> elementToProperties = mapImpl.elementToProperties;

        StreamSupport.stream(elements.spliterator(), false)
                .forEach(element -> {
                    // Update main map of element with group-by properties to properties
                    final Element elementWithGroupByProperties = updateElementToProperties(schema,
                            element, elementToProperties, groupsWithNoAggregation, groupToGroupByProperties,
                            groupToNonGroupByProperties);
                    // Update entitySeedToElements and edgeSeedToElements if index required
                    if (maintainIndex) {
                        updateEntitySeedIndex(entitySeedToElements, elementWithGroupByProperties);
                        updateEdgeSeedIndex(edgeSeedToElements, elementWithGroupByProperties);
                    }
                });
    }

    private Element updateElementToProperties(final Schema schema,
                                              final Element element,
                                              final Map<Element, Properties> elementToProperties,
                                              final Set<String> groupsWithNoAggregation,
                                              final Map<String, Set<String>> groupToGroupByProperties,
                                              final Map<String, Set<String>> groupToNonGroupByProperties) {
        final Element elementForIndexing;
        if (groupsWithNoAggregation.contains(element.getGroup())) {
            elementForIndexing = updateElementToPropertiesNoGroupBy(element, elementToProperties);
        } else {
            elementForIndexing = updateElementToPropertiesWithGroupBy(schema, elementToProperties, groupToGroupByProperties,
                    groupToNonGroupByProperties, element);
        }
        return elementForIndexing;
    }

    private void updateEntitySeedIndex(final Map<EntitySeed, Set<Element>> entitySeedToElements,
                                       final Element elementWithGroupByProperties) {
        if (elementWithGroupByProperties instanceof Entity) {
            final EntitySeed entitySeed = new EntitySeed(((Entity) elementWithGroupByProperties).getVertex());
            updateEntitySeedToElementsMap(entitySeedToElements, entitySeed, elementWithGroupByProperties);
        } else {
            final Edge edge = (Edge) elementWithGroupByProperties;
            final EntitySeed sourceEntitySeed = new EntitySeed(edge.getSource());
            final EntitySeed destinationEntitySeed = new EntitySeed(edge.getDestination());
            updateEntitySeedToElementsMap(entitySeedToElements, sourceEntitySeed, elementWithGroupByProperties);
            updateEntitySeedToElementsMap(entitySeedToElements, destinationEntitySeed, elementWithGroupByProperties);
        }
    }

    private void updateEdgeSeedIndex(final Map<EdgeSeed, Set<Element>> edgeSeedToElements,
                                     final Element elementWithGroupByProperties) {
        if (elementWithGroupByProperties instanceof Edge) {
            final Edge edge = (Edge) elementWithGroupByProperties;
            final EdgeSeed edgeSeed = new EdgeSeed(edge.getSource(), edge.getDestination(), edge.isDirected());
            updateEdgeSeedToElementsMap(edgeSeedToElements, edgeSeed, elementWithGroupByProperties);
        }
    }

    private void updateEntitySeedToElementsMap(final Map<EntitySeed, Set<Element>> entitySeedToElements,
                                               final EntitySeed entitySeed,
                                               final Element element) {
        if (!entitySeedToElements.containsKey(entitySeed)) {
            entitySeedToElements.put(entitySeed, new HashSet<>());
        }
        entitySeedToElements.get(entitySeed).add(element);
    }

    private void updateEdgeSeedToElementsMap(final Map<EdgeSeed, Set<Element>> edgeSeedToElements,
                                             final EdgeSeed edgeSeed,
                                             final Element element) {
        if (!edgeSeedToElements.containsKey(edgeSeed)) {
            edgeSeedToElements.put(edgeSeed, new HashSet<>());
        }
        edgeSeedToElements.get(edgeSeed).add(element);
    }

    private Element updateElementToPropertiesWithGroupBy(final Schema schema,
                                                         final Map<Element, Properties> elementToProperties,
                                                         final Map<String, Set<String>> groupToGroupByProperties,
                                                         final Map<String, Set<String>> groupToNonGroupByProperties,
                                                         final Element element) {
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
        final ElementAggregator aggregator = schema.getElement(group).getAggregator();
        aggregator.initFunctions();
        aggregator.aggregate(existingProperties);
        aggregator.aggregate(properties);
        final Properties aggregatedProperties = new Properties();
        aggregator.state(aggregatedProperties);
        elementToProperties.put(elementWithGroupByProperties, aggregatedProperties);
        return elementWithGroupByProperties;
    }

    private Element updateElementToPropertiesNoGroupBy(final Element element,
                                                       final Map<Element, Properties> elementToProperties) {
        final Properties existingProperties = elementToProperties.get(element);
        if (null == existingProperties) {
            // Clone element and add to map with properties containing 1
            final Element elementWithGroupByProperties = element.emptyClone();
            elementWithGroupByProperties.copyProperties(element.getProperties());
            final Properties properties = new Properties();
            properties.put(MapImpl.COUNT, 1);
            elementToProperties.put(elementWithGroupByProperties, properties);
        } else {
            existingProperties.put(MapImpl.COUNT, ((int) existingProperties.get(MapImpl.COUNT)) + 1);
        }
        return element;
    }
}
