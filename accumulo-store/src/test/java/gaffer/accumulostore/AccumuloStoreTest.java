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

import static gaffer.store.StoreTrait.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Iterables;
import gaffer.accumulostore.operation.handler.GetElementsBetweenSetsHandler;
import gaffer.accumulostore.operation.handler.GetElementsInRangesHandler;
import gaffer.accumulostore.operation.handler.GetElementsWithinSetHandler;
import gaffer.accumulostore.operation.hdfs.handler.AddElementsFromHdfsHandler;
import gaffer.accumulostore.operation.hdfs.handler.ImportAccumuloKeyValueFilesHandler;
import gaffer.accumulostore.operation.hdfs.handler.SampleDataForSplitPointsHandler;
import gaffer.accumulostore.operation.hdfs.handler.SplitTableHandler;
import gaffer.accumulostore.operation.hdfs.operation.ImportAccumuloKeyValueFiles;
import gaffer.accumulostore.operation.hdfs.operation.SampleDataForSplitPoints;
import gaffer.accumulostore.operation.hdfs.operation.SplitTable;
import gaffer.accumulostore.operation.impl.GetEdgesBetweenSets;
import gaffer.accumulostore.operation.impl.GetEdgesInRanges;
import gaffer.accumulostore.operation.impl.GetEdgesWithinSet;
import gaffer.accumulostore.operation.impl.GetElementsBetweenSets;
import gaffer.accumulostore.operation.impl.GetElementsInRanges;
import gaffer.accumulostore.operation.impl.GetElementsWithinSet;
import gaffer.accumulostore.operation.impl.GetEntitiesInRanges;
import gaffer.commonutil.StreamUtil;
import gaffer.commonutil.TestGroups;
import gaffer.commonutil.TestPropertyNames;
import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.function.simple.filter.IsMoreThan;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.Validate;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.generate.GenerateElements;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.operation.impl.get.GetElements;
import gaffer.operation.impl.get.GetElementsBySeed;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.operation.simple.hdfs.operation.AddElementsFromHdfs;
import gaffer.store.StoreException;
import gaffer.store.StoreTrait;
import gaffer.store.operation.handler.OperationHandler;
import gaffer.store.operation.handler.generate.GenerateElementsHandler;
import gaffer.store.operation.handler.generate.GenerateObjectsHandler;
import gaffer.store.schema.Schema;
import gaffer.user.User;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AccumuloStoreTest {

    private static MockAccumuloStore byteEntityStore;
    private static MockAccumuloStore gaffer1KeyStore;
    private static final Schema schema = Schema.fromJson(StreamUtil.schemas(AccumuloStoreTest.class));
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloStoreTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(AccumuloStoreTest.class, "/accumuloStoreClassicKeys.properties"));

    @BeforeClass
    public static void setup() throws StoreException, AccumuloException, AccumuloSecurityException, IOException {
        byteEntityStore = new MockAccumuloStore();
        gaffer1KeyStore = new MockAccumuloStore();
        byteEntityStore.initialise(schema, PROPERTIES);
        gaffer1KeyStore.initialise(schema, CLASSIC_PROPERTIES);

    }

    @AfterClass
    public static void tearDown() {
        byteEntityStore = null;
        gaffer1KeyStore = null;
    }

    @Test
    public void shouldBeAnOrderedStore() {
        assertTrue(byteEntityStore.hasTrait(StoreTrait.ORDERED));
        assertTrue(gaffer1KeyStore.hasTrait(StoreTrait.ORDERED));
    }

    @Test
    public void testAbleToInsertAndRetrieveEntityQueryingEqualAndRelatedGaffer1() throws OperationException {
        testAbleToInsertAndRetrieveEntityQueryingEqualAndRelated(gaffer1KeyStore);
    }

    @Test
    public void testAbleToInsertAndRetrieveEntityQueryingEqualAndRelatedByteEntity() throws OperationException {
        testAbleToInsertAndRetrieveEntityQueryingEqualAndRelated(byteEntityStore);
    }

    public void testAbleToInsertAndRetrieveEntityQueryingEqualAndRelated(AccumuloStore store) throws OperationException {
        final List<Element> elements = new ArrayList<>();
        final Entity e = new Entity(TestGroups.ENTITY, "1");
        e.putProperty(TestPropertyNames.PROP_1, 1);
        e.putProperty(TestPropertyNames.PROP_2, 2);
        e.putProperty(TestPropertyNames.PROP_3, 3);
        e.putProperty(TestPropertyNames.PROP_4, 4);
        e.putProperty(TestPropertyNames.COUNT, 1);

        final User user = new User();
        elements.add(e);
        final AddElements add = new AddElements.Builder()
                .elements(elements)
                .build();
        store.execute(add, user);

        final EntitySeed entitySeed1 = new EntitySeed("1");

        final GetElements<EntitySeed, Element> getBySeed = new GetElementsBySeed.Builder<EntitySeed, Element>()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .addSeed(entitySeed1)
                .build();
        final CloseableIterable<Element> results = store.execute(getBySeed, user);

        assertEquals(1, Iterables.size(results));
        assertThat(results, IsCollectionContaining.hasItem(e));

        final GetRelatedElements<EntitySeed, Element> getRelated = new GetRelatedElements.Builder<EntitySeed, Element>()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .addSeed(entitySeed1)
                .build();
        CloseableIterable<Element> relatedResults = store.execute(getRelated, user);
        assertEquals(1, Iterables.size(relatedResults));
        assertThat(relatedResults, IsCollectionContaining.hasItem(e));

        final GetRelatedElements<EntitySeed, Element> getRelatedWithPostAggregationFilter = new GetRelatedElements.Builder<EntitySeed, Element>()
                .view(new View.Builder()
                    .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                            .preAggregationFilter(new ElementFilter.Builder()
                                    .select(TestPropertyNames.PROP_1)
                                    .execute(new IsMoreThan(0))
                                    .build())
                            .postAggregationFilter(new ElementFilter.Builder()
                                    .select(TestPropertyNames.COUNT)
                                    .execute(new IsMoreThan(6))
                                    .build())
                            .build())
                .build())
                .addSeed(entitySeed1)
                .build();
        relatedResults = store.execute(getRelatedWithPostAggregationFilter, user);
        assertEquals(0, Iterables.size(relatedResults));
    }

    @Test
    public void testStoreReturnsHandlersForRegisteredOperationsGaffer1() throws OperationException, StoreException {
        testStoreReturnsHandlersForRegisteredOperations(gaffer1KeyStore);
    }

    @Test
    public void testStoreReturnsHandlersForRegisteredOperationsByteEntity() throws OperationException, StoreException {
        testStoreReturnsHandlersForRegisteredOperations(byteEntityStore);
    }

    public void testStoreReturnsHandlersForRegisteredOperations(MockAccumuloStore store) throws StoreException {
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
    public void testRequestForNullHandlerManagedGaffer1() throws OperationException {
        testRequestForNullHandlerManaged(gaffer1KeyStore);
    }

    @Test
    public void testRequestForNullHandlerManagedByteEntity() throws OperationException {
        testRequestForNullHandlerManaged(byteEntityStore);
    }


    public void testRequestForNullHandlerManaged(MockAccumuloStore store) {
        final OperationHandler returnedHandler = store.getOperationHandlerExposed(null);
        assertNull(returnedHandler);
    }

    @Test
    public void testStoreTraitsGaffer1() throws OperationException {
        testStoreTraits(gaffer1KeyStore);
    }

    @Test
    public void testStoreTraitsByteEntity() throws OperationException {
        testStoreTraits(byteEntityStore);
    }

    public void testStoreTraits(AccumuloStore store) {
        final Collection<StoreTrait> traits = store.getTraits();
        assertNotNull(traits);
        assertTrue("Collection size should be 8", traits.size() == 8);
        assertTrue("Collection should contain AGGREGATION trait", traits.contains(AGGREGATION));
        assertTrue("Collection should contain PRE_AGGREGATION_FILTERING trait", traits.contains(PRE_AGGREGATION_FILTERING));
        assertTrue("Collection should contain POST_AGGREGATION_FILTERING trait", traits.contains(POST_AGGREGATION_FILTERING));
        assertTrue("Collection should contain TRANSFORMATION trait", traits.contains(TRANSFORMATION));
        assertTrue("Collection should contain POST_TRANSFORMATION_FILTERING trait", traits.contains(POST_TRANSFORMATION_FILTERING));
        assertTrue("Collection should contain STORE_VALIDATION trait", traits.contains(STORE_VALIDATION));
        assertTrue("Collection should contain ORDERED trait", traits.contains(ORDERED));
        assertTrue("Collection should contain VISIBILITY trait", traits.contains(VISIBILITY));
    }

}
