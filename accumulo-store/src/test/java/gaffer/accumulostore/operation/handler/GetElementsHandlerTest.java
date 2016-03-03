/*
 * Copyright 2016 Crown Copyright
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

package gaffer.accumulostore.operation.handler;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.MockAccumuloStore;
import gaffer.accumulostore.MockAccumuloStoreForTest;
import gaffer.accumulostore.utils.AccumuloPropertyNames;
import gaffer.commonutil.StreamUtil;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.GetOperation;
import gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import gaffer.operation.OperationException;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.handler.AbstractGetElementsHandlerTest;
import gaffer.operation.impl.get.GetElements;
import gaffer.store.Store;
import gaffer.store.StoreException;
import gaffer.store.operation.handler.OperationHandler;

public class GetElementsHandlerTest extends AbstractGetElementsHandlerTest {
    @Test
    public void shouldSummariseData() throws IOException, OperationException {
        // Given
        final Store mockStore = createMockStore();
        final OperationHandler<GetElements<ElementSeed, Element>, Iterable<Element>> handler = createHandler();

        final long timestamp = new Date().getTime();
        final List<Entity> entities = new ArrayList<>();
        Entity entity = new Entity(getEntityGroup(), "id1");
        entity.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
        entity.putProperty(AccumuloPropertyNames.TIMESTAMP, timestamp);
        entity.putProperty(AccumuloPropertyNames.COUNT, 1);
        entities.add(entity);

        entity = new Entity(getEntityGroup(), "id1");
        entity.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 2);
        entity.putProperty(AccumuloPropertyNames.TIMESTAMP, timestamp + 1000L);
        entity.putProperty(AccumuloPropertyNames.COUNT, 2);
        entities.add(entity);

        addEntities(entities, mockStore);

        final Iterable<ElementSeed> seeds = Collections.singletonList((ElementSeed) new EntitySeed("id1"));
        final GetElements<ElementSeed, Element> operation =
                createMockOperation(GetOperation.SeedMatchingType.EQUAL, GetOperation.IncludeEdgeType.NONE, true, IncludeIncomingOutgoingType.BOTH, seeds);
        given(operation.isSummarise()).willReturn(true);

        // When
        final Iterable<? extends Element> results = handler.doOperation(operation, mockStore);

        // Then
        assertEquals("The number of elements returned was not as expected. " + Lists.newArrayList(results), 1,
                Lists.newArrayList(results).size());
        final Entity aggregatedEntity = (Entity) results.iterator().next();
        assertEquals(3, aggregatedEntity.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
        assertEquals(3, aggregatedEntity.getProperty(AccumuloPropertyNames.COUNT));
        assertEquals(timestamp, aggregatedEntity.getProperty(AccumuloPropertyNames.TIMESTAMP));
        assertEquals(getEntityGroup(), aggregatedEntity.getGroup());
        assertEquals("id1", aggregatedEntity.getVertex());
    }

    @Test
    public void shouldNotSummariseData() throws IOException, OperationException {
        // Given
        final Store mockStore = createMockStore();
        final OperationHandler<GetElements<ElementSeed, Element>, Iterable<Element>> handler = createHandler();

        final long timestamp = new Date().getTime();
        final List<Entity> entities = new ArrayList<>();
        Entity entity = new Entity(getEntityGroup(), "id1");
        entity.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 1);
        entity.putProperty(AccumuloPropertyNames.TIMESTAMP, timestamp);
        entity.putProperty(AccumuloPropertyNames.COUNT, 1);
        entities.add(entity);

        entity = new Entity(getEntityGroup(), "id1");
        entity.putProperty(AccumuloPropertyNames.COLUMN_QUALIFIER, 2);
        entity.putProperty(AccumuloPropertyNames.TIMESTAMP, timestamp + 1000);
        entity.putProperty(AccumuloPropertyNames.COUNT, 2);
        entities.add(entity);

        addEntities(entities, mockStore);

        final Iterable<ElementSeed> seeds = Collections.singletonList((ElementSeed) new EntitySeed("id1"));
        final GetElements<ElementSeed, Element> operation =
                createMockOperation(GetOperation.SeedMatchingType.EQUAL, GetOperation.IncludeEdgeType.NONE, true, IncludeIncomingOutgoingType.BOTH, seeds);
        given(operation.isSummarise()).willReturn(false);

        // When
        final Iterable<? extends Element> results = handler.doOperation(operation, mockStore);

        // Then
        assertEquals("The number of elements returned was not as expected. " + Lists.newArrayList(results), 2,
                Lists.newArrayList(results).size());
        final Entity aggregatedEntity1 = (Entity) Lists.newArrayList(results).get(0);
        final Entity aggregatedEntity2 = (Entity) Lists.newArrayList(results).get(1);

        assertEquals(1, aggregatedEntity1.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
        assertEquals(1, aggregatedEntity1.getProperty(AccumuloPropertyNames.COUNT));
        assertEquals(timestamp, aggregatedEntity1.getProperty(AccumuloPropertyNames.TIMESTAMP));
        assertEquals(getEntityGroup(), aggregatedEntity1.getGroup());
        assertEquals("id1", aggregatedEntity1.getVertex());

        assertEquals(2, aggregatedEntity2.getProperty(AccumuloPropertyNames.COLUMN_QUALIFIER));
        assertEquals(2, aggregatedEntity2.getProperty(AccumuloPropertyNames.COUNT));
        assertEquals(timestamp + 1000L, aggregatedEntity2.getProperty(AccumuloPropertyNames.TIMESTAMP));
        assertEquals(getEntityGroup(), aggregatedEntity2.getGroup());
        assertEquals("id1", aggregatedEntity2.getVertex());
    }

    @Override
    protected MockAccumuloStore createMockStore() {
        return new MockAccumuloStoreForTest();
    }

    @Override
    protected String getEdgeGroup() {
        return TestGroups.EDGE;
    }

    @Override
    protected String getEntityGroup() {
        return TestGroups.ENTITY;
    }

    @Override
    protected void addEdges(final Collection<Edge> edges, final Store mockStore) {
        try {
            ((AccumuloStore) mockStore).addElements((Collection) edges);
        } catch (StoreException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void addEntities(final Collection<Entity> entities, final Store mockStore) {
        try {
            ((AccumuloStore) mockStore).addElements((Collection) entities);
        } catch (StoreException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected OperationHandler<GetElements<ElementSeed, Element>, Iterable<Element>> createHandler() {
        return new GetElementsHandler();
    }

    @Override
    protected View createView() {
        return View.fromJson(StreamUtil.view(getClass()));
    }
}