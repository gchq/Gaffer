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

import static org.junit.Assert.*;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

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
import gaffer.accumulostore.operation.impl.*;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.Validate;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.generate.GenerateElements;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.operation.impl.get.*;
import gaffer.operation.simple.hdfs.AddElementsFromHdfs;
import gaffer.store.StoreException;
import gaffer.store.StoreProperties;
import gaffer.store.operation.handler.GenerateElementsHandler;
import gaffer.store.operation.handler.GenerateObjectsHandler;
import gaffer.store.operation.handler.OperationHandler;
import gaffer.store.schema.Schema;
import gaffer.store.schema.SchemaEdgeDefinition;
import gaffer.store.schema.SchemaEntityDefinition;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AccumuloStoreTest {

    private static MockAccumuloStoreForTest store;

    @BeforeClass
    public static void setup() throws Exception {
        store = new MockAccumuloStoreForTest();
    }

    @AfterClass
    public static void tearDown() {
        store = null;
    }

    @Test
    public void testAbleToInsertAndRetrieveEntityQueryingEqualAndRelated() throws OperationException {
        List<Element> elements = new ArrayList<>();
        Entity e = new Entity(TestGroups.ENTITY);
        e.setVertex("1");
        elements.add(e);
        AddElements add = new AddElements.Builder()
                .elements(elements)
                .build();
        store.execute(add);

        GetElements<EntitySeed, Element> getBySeed = new GetElementsSeed.Builder<EntitySeed, Element>()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .addSeed(new EntitySeed("1"))
                .build();
        Iterable<Element> results = store.execute(getBySeed);
        Iterator<Element> resultsIter = results.iterator();
        assertTrue(resultsIter.hasNext());
        assertEquals(e, resultsIter.next());
        assertFalse(resultsIter.hasNext());


        GetRelatedElements<EntitySeed, Element> getRelated = new GetRelatedElements.Builder<EntitySeed, Element>()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .addSeed(new EntitySeed("1"))
                .build();
        results = store.execute(getRelated);
        resultsIter = results.iterator();
        assertTrue(resultsIter.hasNext());
        assertEquals(e, resultsIter.next());
        assertFalse(resultsIter.hasNext());
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
}
