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

package gaffer.accumulostore;

import static gaffer.store.StoreTrait.AGGREGATION;
import static gaffer.store.StoreTrait.FILTERING;
import static gaffer.store.StoreTrait.STORE_VALIDATION;
import static gaffer.store.StoreTrait.TRANSFORMATION;
import static org.junit.Assert.*;

import com.google.common.collect.Iterables;
import gaffer.accumulostore.operation.handler.GetElementsBetweenSetsHandler;
import gaffer.accumulostore.operation.handler.GetElementsInRangesHandler;
import gaffer.accumulostore.operation.handler.GetElementsWithinSetHandler;
import gaffer.accumulostore.operation.hdfs.handler.AddElementsFromHdfsHandler;
import gaffer.accumulostore.operation.hdfs.handler.ImportAccumuloKeyValueFilesHandler;
import gaffer.accumulostore.operation.hdfs.handler.SampleDataForSplitPointsHandler;
import gaffer.accumulostore.operation.hdfs.handler.SplitTableHandler;
import gaffer.accumulostore.operation.hdfs.impl.ImportAccumuloKeyValueFiles;
import gaffer.accumulostore.operation.hdfs.impl.SampleDataForSplitPoints;
import gaffer.accumulostore.operation.hdfs.impl.SplitTable;
import gaffer.accumulostore.operation.impl.GetEdgesBetweenSets;
import gaffer.accumulostore.operation.impl.GetEdgesInRanges;
import gaffer.accumulostore.operation.impl.GetEdgesWithinSet;
import gaffer.accumulostore.operation.impl.GetElementsBetweenSets;
import gaffer.accumulostore.operation.impl.GetElementsInRanges;
import gaffer.accumulostore.operation.impl.GetElementsWithinSet;
import gaffer.accumulostore.operation.impl.GetEntitiesInRanges;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.Validate;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.generate.GenerateElements;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.operation.impl.get.GetElements;
import gaffer.operation.impl.get.GetElementsSeed;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.operation.simple.hdfs.AddElementsFromHdfs;
import gaffer.store.StoreException;
import gaffer.store.StoreTrait;
import gaffer.store.operation.handler.GenerateElementsHandler;
import gaffer.store.operation.handler.GenerateObjectsHandler;
import gaffer.store.operation.handler.OperationHandler;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.After;
import gaffer.user.User;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AccumuloStoreTest {

    private MockAccumuloStoreForTest store;

    @Before
    public void setup() throws Exception {
        store = new MockAccumuloStoreForTest();
    }

    @After
    public void tearDown() {
        store = null;
    }

    @Test
    public void testAbleToInsertAndRetrieveEntityQueryingEqualAndRelated() throws OperationException {
        final List<Element> elements = new ArrayList<>();
        final Entity e = new Entity(TestGroups.ENTITY);
        final User user = new User();
        e.setVertex("1");
        elements.add(e);
        final AddElements add = new AddElements.Builder()
                .elements(elements)
                .build();
        store.execute(add, user);

        final EntitySeed entitySeed1 = new EntitySeed("1");

        final GetElements<EntitySeed, Element> getBySeed = new GetElementsSeed.Builder<EntitySeed, Element>()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .addSeed(entitySeed1)
                .build();
        final Iterable<Element> results = store.execute(getBySeed, user);

        assertEquals(1, Iterables.size(results));
        assertThat(results, IsCollectionContaining.hasItem(e));

        final GetRelatedElements<EntitySeed, Element> getRelated = new GetRelatedElements.Builder<EntitySeed, Element>()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .addSeed(entitySeed1)
                .build();
        final Iterable<Element> relatedResults = store.execute(getRelated, user);
        assertEquals(1, Iterables.size(relatedResults));
        assertThat(relatedResults, IsCollectionContaining.hasItem(e));
    }

    @Test
    public void testStoreReturnsHandlersForRegisteredOperations() throws StoreException {
        // Then
        assertNotNull(store.getOperationHandlerExposed(Validate.class));

        assertTrue(store.getOperationHandlerExposed(AddElementsFromHdfs.class) instanceof AddElementsFromHdfsHandler);
        assertTrue(store.getOperationHandlerExposed(GetEdgesBetweenSets.class) instanceof GetElementsBetweenSetsHandler);
        assertTrue(store.getOperationHandlerExposed(GetElementsBetweenSets.class) instanceof GetElementsBetweenSetsHandler);
        assertTrue(store.getOperationHandlerExposed(GetElementsInRanges.class) instanceof GetElementsInRangesHandler);
        assertTrue(store.getOperationHandlerExposed(GetEdgesInRanges.class) instanceof GetElementsInRangesHandler);
        assertTrue(store.getOperationHandlerExposed(GetEntitiesInRanges.class) instanceof GetElementsInRangesHandler);
        assertTrue(store.getOperationHandlerExposed(GetElementsWithinSet.class) instanceof GetElementsWithinSetHandler);
        assertTrue(store.getOperationHandlerExposed(GetEdgesWithinSet.class) instanceof GetElementsWithinSetHandler);
        assertTrue(store.getOperationHandlerExposed(SplitTable.class) instanceof SplitTableHandler);
        assertTrue(store.getOperationHandlerExposed(SampleDataForSplitPoints.class) instanceof SampleDataForSplitPointsHandler);
        assertTrue(store.getOperationHandlerExposed(ImportAccumuloKeyValueFiles.class) instanceof ImportAccumuloKeyValueFilesHandler);
        assertTrue(store.getOperationHandlerExposed(GenerateElements.class) instanceof GenerateElementsHandler);
        assertTrue(store.getOperationHandlerExposed(GenerateObjects.class) instanceof GenerateObjectsHandler);

    }

    @Test
    public void testRequestForNullHandlerManaged() {
        final OperationHandler returnedHandler = store.getOperationHandlerExposed(null);
        assertNull(returnedHandler);
    }

    @Test
    public void testStoreTraits() {
        final Collection<StoreTrait> traits = store.getTraits();
        assertNotNull(traits);
        assertTrue("Collection size should be 4", traits.size() == 4);
        assertTrue("Collection should contain AGGREGATION trait", traits.contains(AGGREGATION));
        assertTrue("Collection should contain FILTERING trait", traits.contains(FILTERING));
        assertTrue("Collection should contain TRANSFORMATION trait", traits.contains(TRANSFORMATION));
        assertTrue("Collection should contain STORE_VALIDATION trait", traits.contains(STORE_VALIDATION));
    }

}
