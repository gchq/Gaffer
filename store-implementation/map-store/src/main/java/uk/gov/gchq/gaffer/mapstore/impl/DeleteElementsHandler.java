/*
 * Copyright 2024 Crown Copyright
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
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.delete.DeleteElements;
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
 * An {@link OperationHandler} for the {@link DeleteElements} operation on the
 * {@link MapStore}.
 */
public class DeleteElementsHandler implements OperationHandler<DeleteElements> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteElementsHandler.class);

  @Override
  public Void doOperation(final DeleteElements deleteElements, final Context context, final Store store)
      throws OperationException {
    Iterable<? extends Element> elements = deleteElements.getInput();
    if (deleteElements.isValidate()) {
      elements = new ValidatedElements(elements, store.getSchema(), deleteElements.isSkipInvalidElements());
    }

    deleteElements(elements, (MapStore) store);
    return null;
  }

  private void deleteElements(final Iterable<? extends Element> elements, final MapStore mapStore) {
    final MapImpl mapImpl = mapStore.getMapImpl();
    final Schema schema = mapStore.getSchema();

    final int bufferSize = mapStore.getProperties().getIngestBufferSize();

    if (bufferSize < 1) {
      // Delete all elements directly
      deleteBatch(mapImpl, schema, elements);
    } else {
      LOGGER.info("Deleting elements in batches, batch size = {}", bufferSize);
      int count = 0;
      final List<Element> batch = new ArrayList<>(bufferSize);
      for (final Element element : elements) {
        if (null != element) {
          batch.add(mapImpl.cloneElement(element, schema));
          count++;
          if (count >= bufferSize) {
            deleteBatch(mapImpl, schema, AggregatorUtil.ingestAggregate(batch, schema));
            batch.clear();
            count = 0;
          }
        }
      }

      if (count > 0) {
        deleteBatch(mapImpl, schema, AggregatorUtil.ingestAggregate(batch, schema));
      }
    }
  }

  private void deleteBatch(final MapImpl mapImpl, final Schema schema, final Iterable<? extends Element> elements) {
    for (final Element element : elements) {
      if (null != element) {
        final Element elementForIndexing = deleteElement(element, schema, mapImpl);

        // Update entityIdToElements and edgeIdToElements if index required
        if (mapImpl.isMaintainIndex()) {
          updateElementIndex(elementForIndexing, mapImpl);
        }
      }
    }
  }

  private Element deleteElement(final Element element, final Schema schema, final MapImpl mapImpl) {
    final Element elementForIndexing;
    if (mapImpl.isAggregationEnabled(element)) {
      elementForIndexing = deleteAggElement(element, mapImpl);
    } else {
      elementForIndexing = deleteNonAggElement(element, schema, mapImpl);
    }
    return elementForIndexing;
  }

  private Element deleteAggElement(final Element element, final MapImpl mapImpl) {
    final String group = element.getGroup();
    final Element elementWithGroupByProperties = element.emptyClone();
    final GroupedProperties properties = new GroupedProperties(element.getGroup());
    if (null != mapImpl.getGroupByProperties(group)) {
      for (final String propertyName : mapImpl.getGroupByProperties(group)) {
        elementWithGroupByProperties.putProperty(propertyName, element.getProperty(propertyName));
      }
    }
    if (null != mapImpl.getNonGroupByProperties(group)) {
      for (final String propertyName : mapImpl.getNonGroupByProperties(group)) {
        properties.put(propertyName, element.getProperty(propertyName));
      }
    }
    mapImpl.deleteAggElement(elementWithGroupByProperties);

    return elementWithGroupByProperties;
  }

  private Element deleteNonAggElement(final Element element, final Schema schema, final MapImpl mapImpl) {
    final Element elementClone = element.emptyClone();

    // Copy properties that exist in the schema
    final SchemaElementDefinition elementDef = schema.getElement(element.getGroup());
    for (final String property : elementDef.getProperties()) {
      elementClone.putProperty(property, element.getProperty(property));
    }

    mapImpl.deleteNonAggElement(elementClone);
    return elementClone;
  }

  private void updateElementIndex(final Element element, final MapImpl mapImpl) {
    if (element instanceof Entity) {
      final Entity entity = (Entity) element;
      final EntitySeed entitySeed = new EntitySeed(entity.getVertex());
      mapImpl.removeIndex(entitySeed, element);
    } else {
      final Edge edge = (Edge) element;
      edge.setIdentifiers(edge.getSource(), edge.getDestination(), edge.isDirected(), EdgeSeed.MatchedVertex.SOURCE);
      final EntitySeed sourceEntitySeed = new EntitySeed(edge.getSource());
      mapImpl.removeIndex(sourceEntitySeed, edge);

      final Edge destMatchedEdge = new Edge(edge.getGroup(), edge.getSource(), edge.getDestination(), edge.isDirected(),
          EdgeSeed.MatchedVertex.DESTINATION, edge.getProperties());
      final EntitySeed destinationEntitySeed = new EntitySeed(edge.getDestination());
      mapImpl.removeIndex(destinationEntitySeed, destMatchedEdge);

      final EdgeSeed edgeSeed = new EdgeSeed(edge.getSource(), edge.getDestination(), edge.isDirected());
      mapImpl.removeIndex(edgeSeed, edge);
    }
  }
}
