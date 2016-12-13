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

package uk.gov.gchq.gaffer.arrayliststore;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.arrayliststore.data.SimpleEdgeDataObject;
import uk.gov.gchq.gaffer.arrayliststore.data.SimpleEntityDataObject;
import uk.gov.gchq.gaffer.arrayliststore.data.generator.SimpleEdgeGenerator;
import uk.gov.gchq.gaffer.arrayliststore.data.generator.SimpleEntityGenerator;
import uk.gov.gchq.gaffer.arrayliststore.data.generator.SimpleGenerator;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.function.filter.IsLessThan;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.data.generator.EntitySeedExtractor;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import uk.gov.gchq.gaffer.operation.impl.get.GetEntities;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ArrayListStoreTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArrayListStoreTest.class);

    @Test
    public void shouldAddAndGetRelatedEdges() throws OperationException {
        final Graph graph = createGraph();
        addElementsToGraph(graph);

        //set up the operation to fetch the edges
        final OperationChain<CloseableIterable<SimpleEdgeDataObject>> opChain = new OperationChain.Builder()
                .first(new GetEdges.Builder<>()
                        .addSeed(new EntitySeed(1))
                        .addSeed(new EntitySeed(2))
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                        .preAggregationFilter(new ElementFilter.Builder()
                                                .select(TestPropertyNames.INT).execute(new IsLessThan(2))
                                                .build())
                                        .build())
                                .build())
                        .build())
                .then(new GenerateObjects.Builder<Edge, SimpleEdgeDataObject>()
                        .generator(new SimpleEdgeGenerator())
                        .build())
                .build();

        //now do the hop
        final CloseableIterable<SimpleEdgeDataObject> results = graph.execute(opChain, new User());

        //check the results by converting our edges back into SimpleDataObjects
        if (!results.iterator().hasNext()) {
            fail("No results returned");
        } else {
            for (final SimpleEdgeDataObject obj : results) {
                LOGGER.info(obj.toString());
            }

            final List<SimpleEdgeDataObject> resultList = Lists.newArrayList(results);
            assertEquals(3, resultList.size());

            int index = 0;
            SimpleEdgeDataObject obj = resultList.get(index++);
            assertEquals(1, obj.getLeft());
            assertEquals(2, obj.getRight());
            assertEquals(1, obj.getVisibility());
            assertEquals("121", obj.getProperties());

            obj = resultList.get(index++);
            assertEquals(2, obj.getLeft());
            assertEquals(3, obj.getRight());
            assertEquals(1, obj.getVisibility());
            assertEquals("231", obj.getProperties());

            obj = resultList.get(index);
            assertEquals(4, obj.getLeft());
            assertEquals(1, obj.getRight());
            assertEquals(1, obj.getVisibility());
            assertEquals("142", obj.getProperties());
        }
        results.close();
    }

    @Test
    public void shouldAddAndGetRelatedEntities() throws OperationException {
        final Graph graph = createGraph();
        addElementsToGraph(graph);

        //set up the operation to fetch the entities
        final OperationChain<CloseableIterable<SimpleEntityDataObject>> opChain = new OperationChain.Builder()
                .first(new GetEntities.Builder<>()
                        .addSeed(new EdgeSeed(2, 1, false))
                        .view(new View.Builder()
                                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                        .preAggregationFilter(new ElementFilter.Builder()
                                                .select(TestPropertyNames.INT).execute(new IsLessThan(2))
                                                .build())
                                        .build())
                                .build())
                        .build())
                .then(new GenerateObjects.Builder<Entity, SimpleEntityDataObject>()
                        .generator(new SimpleEntityGenerator())
                        .build())
                .build();

        //now do the hop
        final CloseableIterable<SimpleEntityDataObject> results = graph.execute(opChain, new User());

        //check the results by converting our edges back into SimpleDataObjects
        if (!results.iterator().hasNext()) {
            fail("No results returned");
        } else {
            for (final SimpleEntityDataObject obj : results) {
                LOGGER.info(obj.toString());
            }

            final List<SimpleEntityDataObject> resultList = Lists.newArrayList(results);
            int index = 0;
            SimpleEntityDataObject obj = resultList.get(index++);
            assertEquals(1, obj.getId());
            assertEquals(1, obj.getVisibility());
            assertEquals("Red", obj.getProperties());

            obj = resultList.get(index);
            assertEquals(2, obj.getId());
            assertEquals(1, obj.getVisibility());
            assertEquals("Orange", obj.getProperties());
        }
        results.close();
    }

    @Test
    public void shouldAddAndGetEntitiesBySeed() throws OperationException {
        final Graph graph = createGraph();
        addElementsToGraph(graph);

        //set up the operation to fetch the entities
        final OperationChain<CloseableIterable<SimpleEntityDataObject>> opChain = new OperationChain.Builder()
                .first(new GetEntities.Builder()
                        .addSeed(new EntitySeed(1))
                        .view(new View.Builder()
                                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                        .preAggregationFilter(new ElementFilter.Builder()
                                                .select(TestPropertyNames.INT).execute(new IsLessThan(2))
                                                .build())
                                        .build())
                                .build())
                        .build())
                .then(new GenerateObjects.Builder<Entity, SimpleEntityDataObject>()
                        .generator(new SimpleEntityGenerator())
                        .build())
                .build();


        //now do the hop
        final CloseableIterable<SimpleEntityDataObject> results = graph.execute(opChain, new User());

        //check the results by converting our edges back into SimpleDataObjects
        if (!results.iterator().hasNext()) {
            fail("No results returned");
        } else {
            for (final SimpleEntityDataObject obj : results) {
                LOGGER.info(obj.toString());
            }

            final List<SimpleEntityDataObject> resultList = Lists.newArrayList(results);
            int index = 0;
            SimpleEntityDataObject obj = resultList.get(index);
            assertEquals(1, obj.getId());
            assertEquals(1, obj.getVisibility());
            assertEquals("Red", obj.getProperties());
        }
        results.close();
    }

    @Test
    public void shouldAddAndGetEdgesBySeed() throws OperationException {
        final Graph graph = createGraph();
        addElementsToGraph(graph);

        //set up the operation to fetch the edges
        final OperationChain<CloseableIterable<SimpleEdgeDataObject>> opChain = new OperationChain.Builder()
                .first(new GetEdges.Builder()
                        .addSeed(new EdgeSeed(2, 1, false))
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                        .preAggregationFilter(new ElementFilter.Builder()
                                                .select(TestPropertyNames.INT).execute(new IsLessThan(2))
                                                .build())
                                        .build())
                                .build())
                        .build())
                .then(new GenerateObjects.Builder<Edge, SimpleEdgeDataObject>()
                        .generator(new SimpleEdgeGenerator())
                        .build())
                .build();


        //now do the hop
        final CloseableIterable<SimpleEdgeDataObject> results = graph.execute(opChain, new User());

        //check the results by converting our edges back into SimpleDataObjects
        if (!results.iterator().hasNext()) {
            fail("No results returned");
        } else {
            for (final SimpleEdgeDataObject obj : results) {
                LOGGER.info(obj.toString());
            }

            final List<SimpleEdgeDataObject> resultList = Lists.newArrayList(results);
            assertEquals(1, resultList.size());

            int index = 0;
            SimpleEdgeDataObject obj = resultList.get(index);
            assertEquals(1, obj.getLeft());
            assertEquals(2, obj.getRight());
            assertEquals(1, obj.getVisibility());
            assertEquals("121", obj.getProperties());
        }
        results.close();
    }

    @Test
    public void shouldAddAndGetEdgesThenEntities() throws OperationException {
        final Graph graph = createGraph();
        addElementsToGraph(graph);

        //set up the operation to fetch the entities
        final OperationChain<CloseableIterable<SimpleEntityDataObject>> opChain = new OperationChain.Builder()
                .first(new GetEdges.Builder<>()
                        .addSeed(new EntitySeed(1))
                        .build())
                .then(new GenerateObjects.Builder<Edge, EntitySeed>()
                        .generator(new EntitySeedExtractor(IdentifierType.DESTINATION))
                        .build())
                .then(new GetEntities.Builder()
                        .view(new View.Builder()
                                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                        .preAggregationFilter(new ElementFilter.Builder()
                                                .select(TestPropertyNames.INT).execute(new IsLessThan(2))
                                                .build())
                                        .build())
                                .build())
                        .build())
                .then(new GenerateObjects.Builder<Entity, SimpleEntityDataObject>()
                        .generator(new SimpleEntityGenerator())
                        .build())
                .build();

        //now do the hop
        final CloseableIterable<SimpleEntityDataObject> results = graph.execute(opChain, new User());

        //check the results by converting our edges back into SimpleDataObjects
        if (!results.iterator().hasNext()) {
            fail("No results returned");
        } else {
            for (final SimpleEntityDataObject obj : results) {
                LOGGER.info(obj.toString());
            }

            final List<SimpleEntityDataObject> resultList = Lists.newArrayList(results);
            assertEquals(1, resultList.size());
            assertEquals(1, resultList.get(0).getId());
            assertEquals(1, resultList.get(0).getVisibility());
            assertEquals("Red", resultList.get(0).getProperties());
        }
        results.close();
    }

    private Graph createGraph() {
        return new Graph.Builder()
                .storeProperties(StreamUtil.storeProps(getClass()))
                .addSchema(StreamUtil.schema(getClass()))
                .build();
    }

    private void addElementsToGraph(final Graph graph) throws OperationException {
        final OperationChain<Void> opChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<>()
                        .objects(getDomainObjects())
                        .generator(new SimpleGenerator())
                        .build())
                .then(new AddElements())
                .build();

        // execute the operation
        graph.execute(opChain, new User());
    }

    private ArrayList<Object> getDomainObjects() {
        ArrayList<Object> domainObjs = new ArrayList<>();
        domainObjs.add(new SimpleEntityDataObject(1, 1, "Red"));
        domainObjs.add(new SimpleEntityDataObject(2, 1, "Orange"));
        domainObjs.add(new SimpleEntityDataObject(3, 2, "Yellow"));
        domainObjs.add(new SimpleEntityDataObject(4, 2, "Green"));

        domainObjs.add(new SimpleEdgeDataObject(1, 2, 1, "121"));
        domainObjs.add(new SimpleEdgeDataObject(2, 3, 1, "231"));
        domainObjs.add(new SimpleEdgeDataObject(3, 4, 1, "342"));
        domainObjs.add(new SimpleEdgeDataObject(4, 1, 1, "142"));
        domainObjs.add(new SimpleEdgeDataObject(1, 3, 2, "132"));
        domainObjs.add(new SimpleEdgeDataObject(2, 4, 2, "242"));
        return domainObjs;
    }
}