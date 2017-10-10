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
package uk.gov.gchq.gaffer.integration.graph;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.Graph.Builder;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.Max;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringDeduplicateConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.predicate.IsTrue;

import java.util.Arrays;
import java.util.List;

/**
 * Integration tests to check that an store can be configured with a schema
 * containing groups 1 and 2. Then a new Graph can be constructed with a limited
 * schema, perhaps just containing group 1. The graph should then just
 * completely hide group 2 and never read any group 2 data from the store.
 */
public abstract class SchemaHidingIT {
    private static final User USER = new User.Builder()
            .dataAuth("public")
            .build();

    protected final String storePropertiesPath;

    public SchemaHidingIT(final String storePropertiesPath) {
        this.storePropertiesPath = storePropertiesPath;
    }

    @Before
    public void before() {
        cleanUp();
    }


    @After
    public void after() {
        cleanUp();
    }

    protected abstract void cleanUp();

    @SuppressWarnings("unchecked")
    @Test
    public void shouldCreateStoreWithFullSchemaAndThenBeAbleUseASubsetOfTheSchema() throws Exception {
        // Add some data to the full graph
        final Store fullStore = Store.createStore("graphId", createFullSchema(), StoreProperties.loadStoreProperties(StreamUtil.openStream(getClass(), storePropertiesPath)));
        final Graph fullGraph = new Builder()
                .store(fullStore)
                .build();


        final Edge edge1a = new Edge.Builder()
                .source("source1a")
                .dest("dest1a")
                .directed(true)
                .group(TestGroups.EDGE)
                .property(TestPropertyNames.COUNT, 1)
                .property(TestPropertyNames.PROP_1, "1a")
                .property(TestPropertyNames.VISIBILITY, "public")
                .property(TestPropertyNames.TIMESTAMP, 1L)
                .build();
        final Edge edge1b = new Edge.Builder()
                .source("source1b")
                .dest("dest1b")
                .directed(true)
                .group(TestGroups.EDGE)
                .property(TestPropertyNames.COUNT, 1)
                .property(TestPropertyNames.PROP_1, "1b")
                .property(TestPropertyNames.VISIBILITY, "public")
                .property(TestPropertyNames.TIMESTAMP, 1L)
                .build();
        final Edge edge2 = new Edge.Builder()
                .source("source2")
                .dest("dest2")
                .directed(true)
                .group(TestGroups.EDGE_2)
                .property(TestPropertyNames.COUNT, 1)
                .property(TestPropertyNames.PROP_1, "2")
                .property(TestPropertyNames.VISIBILITY, "public")
                .property(TestPropertyNames.TIMESTAMP, 1L)
                .build();

        fullGraph.execute(new AddElements.Builder()
                        .input(edge1a, edge1b, edge2)
                        .build(),
                USER);


        // Create a graph a hidden group backed by the same accumulo instance
        final Store filteredStore = Store.createStore("graphId", createFilteredSchema(), StoreProperties.loadStoreProperties(StreamUtil.openStream(getClass(), storePropertiesPath)));
        final Graph filteredGraph = new Builder()
                .store(filteredStore)
                .build();

        final List<Edge> fullExpectedResults = Arrays.asList(edge1a, edge1b, edge2);
        final List<Edge> filteredExpectedResults = Arrays.asList(edge1a, edge1b);

        // Run operations and check the hidden group is hidden when the operation is run on the filtered graph

        testOperations(fullGraph, filteredGraph, fullExpectedResults, filteredExpectedResults);
    }

    protected void testOperations(final Graph fullGraph, final Graph filteredGraph, final List<Edge> fullExpectedResults, final List<Edge> filteredExpectedResults) throws uk.gov.gchq.gaffer.operation.OperationException {
        testOperation(fullGraph, filteredGraph, new GetAllElements(), fullExpectedResults, filteredExpectedResults);

        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("source1a"),
                        new EntitySeed("source1b"),
                        new EntitySeed("source2"),
                        new EdgeSeed("source2", "dest2", true))
                .build();
        testOperation(fullGraph, filteredGraph, getElements, fullExpectedResults, filteredExpectedResults);
    }

    protected void testOperation(final Graph fullGraph, final Graph filteredGraph,
                                 final Output<CloseableIterable<? extends Element>> operation,
                                 final List<Edge> fullExpectedResults,
                                 final List<Edge> filteredExpectedResults)
            throws uk.gov.gchq.gaffer.operation.OperationException {

        // When
        final Iterable<? extends Element> fullResults = fullGraph.execute(operation, USER);
        final Iterable<? extends Element> filteredResults = filteredGraph.execute(operation, USER);

        // Then
        ElementUtil.assertElementEquals(fullExpectedResults, fullResults);
        ElementUtil.assertElementEquals(filteredExpectedResults, filteredResults);
    }

    protected Schema createFullSchema() {
        return new Schema.Builder()
                .merge(createFilteredSchema())
                .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .property(TestPropertyNames.VISIBILITY, "visibility")
                        .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                        .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                        .groupBy(TestPropertyNames.PROP_1)
                        .build())
                .build();
    }

    protected Schema createFilteredSchema() {
        return new Schema.Builder()
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type(TestTypes.DIRECTED_TRUE, new TypeDefinition.Builder()
                        .clazz(Boolean.class)
                        .validateFunctions(new IsTrue())
                        .build())
                .type(TestTypes.PROP_COUNT, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .aggregateFunction(new Sum())
                        .build())
                .type(TestTypes.PROP_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(new StringDeduplicateConcat())
                        .build())
                .type(TestTypes.VISIBILITY, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(new StringConcat())
                        .serialiser(new StringSerialiser())
                        .build())
                .type(TestTypes.TIMESTAMP, new TypeDefinition.Builder()
                        .clazz(Long.class)
                        .aggregateFunction(new Max())
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .property(TestPropertyNames.VISIBILITY, "visibility")
                        .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                        .property(TestPropertyNames.PROP_1, TestTypes.PROP_STRING)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                        .groupBy(TestPropertyNames.PROP_1)
                        .build())
                .visibilityProperty(TestPropertyNames.VISIBILITY)
                .build();
    }
}
