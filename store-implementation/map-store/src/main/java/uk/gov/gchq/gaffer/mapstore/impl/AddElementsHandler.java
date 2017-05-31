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

import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
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

/**
 * An {@link OperationHandler} for the {@link AddElements} operation on the {@link MapStore}.
 */
public class AddElementsHandler implements OperationHandler<AddElements> {

    @Override
    public Void doOperation(final AddElements addElements, final Context context, final Store store) throws OperationException {
        doOperation(addElements, (MapStore) store);
        return null;
    }

    private void doOperation(final AddElements addElements, final MapStore mapStore) {
        final MapImpl mapImpl = mapStore.getMapImpl();
        addElements(addElements.getInput(), mapImpl, mapStore.getSchema());
    }

    private void addElements(final Iterable<? extends Element> elements, final MapImpl mapImpl, final Schema schema) {
        final boolean maintainIndex = mapImpl.maintainIndex;
        final Set<String> groupsWithNoAggregation = mapImpl.groupsWithNoAggregation;
        final Map<EntityId, Set<Element>> entityIdToElements = mapImpl.entityIdToElements;
        final Map<EdgeId, Set<Element>> edgeIdToElements = mapImpl.edgeIdToElements;
        final Map<String, Set<String>> groupToGroupByProperties = mapImpl.groupToGroupByProperties;
        final Map<String, Set<String>> groupToNonGroupByProperties = mapImpl.groupToNonGroupByProperties;
        final Map<Element, Properties> elementToProperties = mapImpl.elementToProperties;

        Streams.toStream(elements)
                .forEach(element -> {
                    // Update main map of element with group-by properties to properties
                    final Element elementWithGroupByProperties = updateElementToProperties(schema,
                            element, elementToProperties, groupsWithNoAggregation, groupToGroupByProperties,
                            groupToNonGroupByProperties);
                    // Update entityIdToElements and edgeIdToElements if index required
                    if (maintainIndex) {
                        updateEntityIdIndex(entityIdToElements, elementWithGroupByProperties);
                        updateEdgeIdIndex(edgeIdToElements, elementWithGroupByProperties);
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

    private void updateEntityIdIndex(final Map<EntityId, Set<Element>> entityIdToElements,
                                     final Element elementWithGroupByProperties) {
        if (elementWithGroupByProperties instanceof Entity) {
            final EntityId entityId = new EntitySeed(((Entity) elementWithGroupByProperties).getVertex());
            updateEntityIdToElementsMap(entityIdToElements, entityId, elementWithGroupByProperties);
        } else {
            final Edge edge = (Edge) elementWithGroupByProperties;
            final EntityId sourceEntityId = new EntitySeed(edge.getSource());
            final EntityId destinationEntityId = new EntitySeed(edge.getDestination());
            updateEntityIdToElementsMap(entityIdToElements, sourceEntityId, elementWithGroupByProperties);
            updateEntityIdToElementsMap(entityIdToElements, destinationEntityId, elementWithGroupByProperties);
        }
    }

    private void updateEdgeIdIndex(final Map<EdgeId, Set<Element>> edgeIdToElements,
                                   final Element elementWithGroupByProperties) {
        if (elementWithGroupByProperties instanceof Edge) {
            final Edge edge = (Edge) elementWithGroupByProperties;
            final EdgeId edgeId = new EdgeSeed(edge.getSource(), edge.getDestination(), edge.isDirected());
            updateEdgeIdToElementsMap(edgeIdToElements, edgeId, elementWithGroupByProperties);
        }
    }

    private void updateEntityIdToElementsMap(final Map<EntityId, Set<Element>> entityIdToElements,
                                             final EntityId entityId,
                                             final Element element) {
        Set<Element> elements = entityIdToElements.get(entityId);
        if (null == elements) {
            elements = new HashSet<>();
            entityIdToElements.put(entityId, elements);
        }
        elements.add(element);
    }

    private void updateEdgeIdToElementsMap(final Map<EdgeId, Set<Element>> edgeIdToElements,
                                           final EdgeId edgeId,
                                           final Element element) {
        Set<Element> elements = edgeIdToElements.get(edgeId);
        if (null == elements) {
            elements = new HashSet<>();
            edgeIdToElements.put(edgeId, elements);
        }
        elements.add(element);
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

        Properties existingProperties = elementToProperties.get(elementWithGroupByProperties);
        if (null == existingProperties) {
            existingProperties = new Properties();
            elementToProperties.put(elementWithGroupByProperties, existingProperties);
        }

        schema.getElement(group).getAggregator().apply(existingProperties, properties);
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
