/*
 * Copyright 2017-2018 Crown Copyright
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
import uk.gov.gchq.gaffer.store.ValidatedElements;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.util.AggregatorUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * An {@link OperationHandler} for the {@link AddElements} operation on the {@link MapStore}.
 */
public class AddElementsHandler implements OperationHandler<AddElements> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AddElementsHandler.class);

    @Override
    public Void doOperation(final AddElements addElements, final Context context, final Store store) throws OperationException {
        Iterable<? extends Element> elements = addElements.getInput();
        if (addElements.isValidate()) {
            elements = new ValidatedElements(elements, store.getSchema(), addElements.isSkipInvalidElements());
        }

        addElements(elements, (MapStore) store);
        return null;
    }

    private void addElements(final Iterable<? extends Element> elements, final MapStore mapStore) {
        final MapImpl mapImpl = mapStore.getMapImpl();
        final Schema schema = mapStore.getSchema();

        final int bufferSize = mapStore.getProperties().getIngestBufferSize();

        if (bufferSize < 1) {
            // Add all elements directly
            addBatch(mapImpl, schema, elements);
        } else {
            LOGGER.info("Adding elements in batches, batch size = " + bufferSize);
            int count = 0;
            final List<Element> batch = new ArrayList<>(bufferSize);
            for (final Element element : elements) {
                if (null != element) {
                    batch.add(element);
                    count++;
                    if (count >= bufferSize) {
                        addBatch(mapImpl, schema, AggregatorUtil.ingestAggregate(batch, schema));
                        batch.clear();
                        count = 0;
                    }
                }
            }

            if (count > 0) {
                addBatch(mapImpl, schema, AggregatorUtil.ingestAggregate(batch, schema));
            }
        }
    }

    private void addBatch(final MapImpl mapImpl, final Schema schema, final Iterable<? extends Element> elements) {
        for (final Element element : elements) {
            if (null != element) {
                final Element elementForIndexing = addElement(element, schema, mapImpl);

                // Update entityIdToElements and edgeIdToElements if index required
                if (mapImpl.isMaintainIndex()) {
                    updateElementIndex(elementForIndexing, mapImpl);
                }
            }
        }
    }

    private Element addElement(final Element element, final Schema schema, final MapImpl mapImpl) {
        final Element elementForIndexing;
        if (!mapImpl.isAggregationEnabled(element)) {
            elementForIndexing = addNonAggElement(element, schema, mapImpl);
        } else {
            elementForIndexing = addAggElement(element, schema, mapImpl);
        }
        return elementForIndexing;
    }

    private Element addAggElement(final Element element, final Schema schema, final MapImpl mapImpl) {
        final String group = element.getGroup();
        final Element elementWithGroupByProperties = element.emptyClone();
        final GroupedProperties properties = new GroupedProperties(element.getGroup());
        for (final String propertyName : mapImpl.getGroupByProperties(group)) {
            elementWithGroupByProperties.putProperty(propertyName, element.getProperty(propertyName));
        }
        for (final String propertyName : mapImpl.getNonGroupByProperties(group)) {
            properties.put(propertyName, element.getProperty(propertyName));
        }

        mapImpl.addAggElement(elementWithGroupByProperties, properties);
        return elementWithGroupByProperties;
    }

    private Element addNonAggElement(final Element element, final Schema schema, final MapImpl mapImpl) {
        final Element elementClone = element.emptyClone();

        // Copy properties that exist in the schema
        final SchemaElementDefinition elementDef = schema.getElement(element.getGroup());
        for (final String property : elementDef.getProperties()) {
            elementClone.putProperty(property, element.getProperty(property));
        }

        mapImpl.addNonAggElement(elementClone);
        return elementClone;
    }

    private void updateElementIndex(final Element element, final MapImpl mapImpl) {
        if (element instanceof Entity) {
            final Entity entity = (Entity) element;
            final EntityId entityId = new EntitySeed(entity.getVertex());
            mapImpl.addIndex(entityId, element);
        } else {
            final Edge edge = (Edge) element;
            edge.setIdentifiers(edge.getSource(), edge.getDestination(), edge.isDirected(), EdgeId.MatchedVertex.SOURCE);
            final EntityId sourceEntityId = new EntitySeed(edge.getSource());
            mapImpl.addIndex(sourceEntityId, edge);

            final Edge destMatchedEdge = new Edge(edge.getGroup(), edge.getSource(), edge.getDestination(), edge.isDirected(), EdgeId.MatchedVertex.DESTINATION, edge.getProperties());
            final EntityId destinationEntityId = new EntitySeed(edge.getDestination());
            mapImpl.addIndex(destinationEntityId, destMatchedEdge);

            final EdgeId edgeId = new EdgeSeed(edge.getSource(), edge.getDestination(), edge.isDirected());
            mapImpl.addIndex(edgeId, edge);
        }
    }
}
