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

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.GroupedProperties;
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
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.util.AggregatorUtil;
import java.util.Map;

/**
 * An {@link OperationHandler} for the {@link AddElements} operation on the {@link MapStore}.
 */
public class AddElementsHandler implements OperationHandler<AddElements> {
    @Override
    public Void doOperation(final AddElements addElements, final Context context, final Store store) throws OperationException {
        doOperation(addElements, (MapStore) store);
        return null;
    }

    private void doOperation(final AddElements addElements, final MapStore mapStore) throws OperationException {
        addElements(addElements.getInput(), mapStore);
    }

    private void addElements(final Iterable<? extends Element> elements, final MapStore mapStore) {
        final MapImpl mapImpl = mapStore.getMapImpl();
        final Schema schema = mapStore.getSchema();

        // Aggregate the new elements before adding them to the map.
        final Iterable<Element> aggregatedElements = AggregatorUtil.ingestAggregate(elements, schema);

        for (final Element element : aggregatedElements) {
            if (null != element) {
                final Element elementForIndexing = updateElements(element, schema, mapImpl);

                // Update entityIdToElements and edgeIdToElements if index required
                if (mapImpl.maintainIndex) {
                    updateIdIndexes(elementForIndexing, mapImpl);
                }
            }
        }
    }

    private Element updateElements(final Element element, final Schema schema, final MapImpl mapImpl) {
        final Element elementForIndexing;
        if (!mapImpl.isAggregationEnabled(element)) {
            elementForIndexing = updateNonAggElements(element, schema, mapImpl);
        } else {
            elementForIndexing = updateAggElements(element, schema, mapImpl);
        }
        return elementForIndexing;
    }

    private void updateIdIndexes(final Element element, final MapImpl mapImpl) {
        if (element instanceof Entity) {
            final Entity entity = (Entity) element;
            final EntityId entityId = new EntitySeed(entity.getVertex());
            mapImpl.entityIdToElements.put(entityId, element);
        } else {
            final Edge edge = (Edge) element;
            final EntityId sourceEntityId = new EntitySeed(edge.getSource());
            final EntityId destinationEntityId = new EntitySeed(edge.getDestination());
            mapImpl.entityIdToElements.put(sourceEntityId, element);
            mapImpl.entityIdToElements.put(destinationEntityId, element);

            final EdgeId edgeId = new EdgeSeed(edge.getSource(), edge.getDestination(), edge.isDirected());
            mapImpl.edgeIdToElements.put(edgeId, edge);
        }
    }

    private Element updateAggElements(final Element element, final Schema schema, final MapImpl mapImpl) {
        final String group = element.getGroup();
        final Element elementWithGroupByProperties = element.emptyClone();
        final GroupedProperties properties = new GroupedProperties(element.getGroup());
        for (final String propertyName : mapImpl.getGroupByProperties(group)) {
            elementWithGroupByProperties.putProperty(propertyName, element.getProperty(propertyName));
        }
        for (final String propertyName : mapImpl.getNonGroupByProperties(group)) {
            properties.put(propertyName, element.getProperty(propertyName));
        }

        final Map<Element, GroupedProperties> map = mapImpl.aggElements.get(elementWithGroupByProperties.getGroup());
        map.merge(elementWithGroupByProperties, properties, new AggregatorUtil.PropertiesBinaryOperator(schema));

        return elementWithGroupByProperties;
    }

    private Element updateNonAggElements(final Element element, final Schema schema, final MapImpl mapImpl) {
        final Element elementClone = element.emptyClone();

        // Copy properties that exist in the schema
        final SchemaElementDefinition elementDef = schema.getElement(element.getGroup());
        if (elementDef.getProperties().equals(element.getProperties().keySet())) {
            elementClone.copyProperties(element.getProperties());
        } else {
            for (final String property : elementDef.getProperties()) {
                elementClone.putProperty(property, element.getProperty(property));
            }
        }

        final Map<Element, Integer> map = mapImpl.nonAggElements.get(elementClone.getGroup());
        map.merge(elementClone, 1, (a, b) -> a + b);

        return elementClone;
    }
}
