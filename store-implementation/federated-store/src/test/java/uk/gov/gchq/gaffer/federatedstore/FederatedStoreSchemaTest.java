/*
 * Copyright 2017-2022 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.predicate.Exists;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.DEST_BASIC;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_EDGE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_2;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SOURCE_BASIC;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.VALUE_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.VALUE_2;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextTestUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.property;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedOperation;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedStoreSchemaTest {
    public static final String GRAPH_ID_A = "a";
    public static final String GRAPH_ID_B = "b";
    public static final String GRAPH_ID_C = "c";
    public static final String DEST_2 = DEST_BASIC + 2;
    public static final AccumuloProperties STORE_PROPERTIES = loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);
    private static final Schema STRING_TYPE = new Schema.Builder()
            .type(STRING, new TypeDefinition.Builder()
                    .clazz(String.class)
                    .aggregateFunction(new StringConcat())
                    .build())
            .build();
    private static final Schema STRING_REQUIRED_TYPE = new Schema.Builder()
            .type(STRING, new TypeDefinition.Builder()
                    .clazz(String.class)
                    .aggregateFunction(new StringConcat())
                    .validateFunctions(new Exists())
                    .build())
            .build();
    public User testUser;
    public Context testContext;
    private FederatedStore federatedStore;

    @BeforeEach
    public void setUp() throws Exception {
        resetForFederatedTests();

        federatedStore = new FederatedStore();
        federatedStore.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, new FederatedStoreProperties());

        testUser = testUser();
        testContext = contextTestUser();
    }

    @Test
    public void shouldBeAbleToAddGraphsWithSchemaCollisions() throws OperationException {
        // Given
        addGroupCollisionGraphs();
        addGraphWith(GRAPH_ID_C, STRING_TYPE, PROPERTY_1);

        // When
        final Collection<String> graphIds = federatedStore.getAllGraphIds(testUser);

        // Then
        assertThat(graphIds).isEqualTo(new HashSet<>(Arrays.asList(GRAPH_ID_A, GRAPH_ID_B, GRAPH_ID_C)));
    }


    @Test
    public void shouldGetCorrectDefaultViewForAChosenGraphOperation() throws OperationException {
        // Given
        addGroupCollisionGraphs();

        // When
        final Iterable<? extends Element> allElements = federatedStore.execute(new OperationChain.Builder()
                .first(getFederatedOperation(new GetAllElements.Builder()
                        // No view so makes default view, should get only view compatible with graph "a"
                        .build())
                        .graphIdsCSV(GRAPH_ID_A))
                .build(), testContext);

        // Then
        assertThat(allElements).isNotNull();
        assertThat(allElements.iterator()).isExhausted();
    }

    @Test
    public void shouldBeAbleToGetElementsWithOverlappingSchemas() throws OperationException {
        // Given
        addOverlappingPropertiesGraphs(STRING_TYPE);

        // Element 1
        addEdgeBasicWith(DEST_BASIC, 1);

        // Element 2
        addEdgeBasicWith(DEST_2, 1, 2);

        // When
        final Iterable<? extends Element> results = federatedStore.execute(new GetElements.Builder()
                .input(new EntitySeed(SOURCE_BASIC))
                .view(new View.Builder()
                        .edge(GROUP_BASIC_EDGE, new ViewElementDefinition.Builder()
                                .properties(PROPERTY_2)
                                .build())
                        .build())
                .build(), testContext);

        // Then
        final HashSet<Edge> expected = new HashSet<>();
        // Graph a, element 1: prop2 missing
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .build());
        // Graph a, element 2: prop2 missing
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_2)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .build());
        // Graph b, element 1: prop2 empty (see below)
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                // Due to a string serialisation quirk, missing properties (null value)
                // are deserialsed as empty strings
                // This will be fixed so the test will need amending, as per gh-2483
                .property(PROPERTY_2, "")
                .build());
        // Graph b, element 2: prop2 present
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_2)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_2, VALUE_2)
                .build());

        assertThat((Iterable<Edge>) results)
                .isNotNull()
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void shouldBeAbleToGetSchemaWithOverlappingSchemas() throws OperationException {
        // Given
        addOverlappingPropertiesGraphs(STRING_TYPE);

        // When
        final Schema schema = federatedStore.execute(new GetSchema.Builder().build(), testContext);

        // Then
        assertThat(schema.validate().isValid()).withFailMessage(schema.validate().getErrorString()).isTrue();
    }

    @Test
    public void shouldValidateCorrectlyWithOverlappingSchemas() throws OperationException {
        // Given
        addOverlappingPropertiesGraphs(STRING_REQUIRED_TYPE);

        // When
        addEdgeBasicWith(DEST_2, 1, 2);

        final Iterable<? extends Element> results = federatedStore.execute(new GetAllElements.Builder().build(), testContext);

        // Then
        final HashSet<Edge> expected = new HashSet<>();
        // Graph a
        expected.add(edgeBasicWith(DEST_2, 1));
        // Graph b
        expected.add(edgeBasicWith(DEST_2, 1, 2));

        assertThat((Iterable<Edge>) results)
                .isNotNull()
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void shouldThrowValidationMissingPropertyWithOverlappingSchemas() throws OperationException {
        // Given
        addOverlappingPropertiesGraphs(STRING_REQUIRED_TYPE);

        // Then
        assertThatExceptionOfType(OperationException.class)
                .isThrownBy(() -> {
                    addEdgeBasicWith(DEST_2, 1);
                })
                .withStackTraceContaining("returned false for properties: {%s: null}", property(2));
    }

    @Test
    public void shouldBeAbleToIngestAggregateWithOverlappingSchemas() throws OperationException {
        // Given
        addOverlappingPropertiesGraphs(STRING_TYPE);

        // Element 1
        federatedStore.execute(new AddElements.Builder()
                .input(edgeBasicWith(DEST_BASIC, 1, 2))
                .build(), testContext);

        // Element 2
        federatedStore.execute(new AddElements.Builder()
                .input(edgeBasicWith(DEST_BASIC, 1, 2))
                .build(), testContext);

        // When
        final Iterable<? extends Element> results = federatedStore.execute(new GetElements.Builder()
                .input(new EntitySeed(SOURCE_BASIC))
                .view(new View.Builder()
                        .edge(GROUP_BASIC_EDGE, new ViewElementDefinition.Builder().build()).build())
                .build(), testContext);

        // Then
        final HashSet<Edge> expected = new HashSet<>();
        // Graph a: prop1 aggregated, prop2 missing
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, "value1,value1")
                .build());
        // Graph b: prop1 aggregated, prop2 aggregated
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, "value1,value1")
                .property(PROPERTY_2, "value2,value2")
                .build());

        assertThat((Iterable<Edge>) results)
                .isNotNull()
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void shouldBeAbleToIngestAggregateMissingPropertyWithOverlappingSchemas() throws OperationException {
        // Given
        addOverlappingPropertiesGraphs(STRING_TYPE);

        // Element 1
        addEdgeBasicWith(DEST_BASIC, 1);

        // Element 2
        addEdgeBasicWith(DEST_BASIC, 1);

        // When
        final Iterable<? extends Element> elements = federatedStore.execute(new GetElements.Builder()
                .input(new EntitySeed(SOURCE_BASIC))
                .view(new View.Builder()
                        .edge(GROUP_BASIC_EDGE, new ViewElementDefinition.Builder().build()).build())
                .build(), testContext);

        // Then
        final HashSet<Edge> expected = new HashSet<>();
        // Graph a: prop1 aggregated, prop2 missing
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, "value1,value1")
                .build());
        // Graph b: prop1 aggregated, prop2 aggregated empty (see below)
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, "value1,value1")
                // Due to a string serialisation quirk, missing properties (null value)
                // are deserialsed as empty strings, so here 2 empty strings are aggregated
                // This will be fixed so the test will need amending, as per gh-2483
                .property(PROPERTY_2, ",")
                .build());

        assertThat((Iterable<Edge>) elements)
                .isNotNull()
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void shouldBeAbleToViewPropertyWithOverlappingSchemas() throws OperationException {
        // Given
        addOverlappingPropertiesGraphs(STRING_TYPE);

        // Element 1
        addEdgeBasicWith(DEST_BASIC, 1, 2);

        // Element 2
        addEdgeBasicWith(DEST_2, 1, 2);


        // When
        final Iterable<? extends Element> results = federatedStore.execute(new GetElements.Builder()
                .input(new EntitySeed(SOURCE_BASIC))
                .view(new View.Builder()
                        .edge(GROUP_BASIC_EDGE, new ViewElementDefinition.Builder()
                                .properties(PROPERTY_2)
                                .build())
                        .build())
                .build(), testContext);

        // Then
        final HashSet<Edge> expected = new HashSet<>();
        // Graph a, element 1: prop1 omitted, prop2 missing
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .build());
        // Graph a, element 2: prop1 omitted, prop2 missing
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_2)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .build());
        // Graph b, element 1: prop1 omitted, prop2 present
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_2, VALUE_2)
                .build());
        // Graph b, element 2: prop1 omitted, prop2 present
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_2)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_2, VALUE_2)
                .build());

        assertThat((Iterable<Edge>) results)
                .isNotNull()
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void shouldBeAbleToFilterPropertyWithOverlappingSchemas() throws OperationException {
        // Given
        addOverlappingPropertiesGraphs(STRING_TYPE);

        // Element 1
        addEdgeBasicWith(DEST_BASIC, 1, 2);

        // Element 2
        addEdgeBasicWith(DEST_2, 1, 2);


        // When
        final Iterable<? extends Element> results = federatedStore.execute(new GetElements.Builder()
                .input(new EntitySeed(SOURCE_BASIC))
                .view(new View.Builder()
                        .edge(GROUP_BASIC_EDGE, new ViewElementDefinition.Builder()
                                .postAggregationFilter(new ElementFilter.Builder()
                                        .select(PROPERTY_2)
                                        .execute(new IsEqual(VALUE_2))
                                        .build())
                                .build())
                        .build())
                .build(), testContext);

        // Then
        final HashSet<Edge> expected = new HashSet<>();
        // Graph b, element 1
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, VALUE_1)
                .property(PROPERTY_2, VALUE_2)
                .build());
        // Graph b, element 2
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_2)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, VALUE_1)
                .property(PROPERTY_2, VALUE_2)
                .build());

        assertThat((Iterable<Edge>) results)
                .isNotNull()
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    public void shouldBeAbleToQueryAggregatePropertyWithOverlappingSchemas() throws OperationException {
        // Given
        addOverlappingPropertiesGraphs(STRING_TYPE);

        // Element 1
        addEdgeBasicWith(DEST_BASIC, 1, 2);

        // Element 2
        addEdgeBasicWith(DEST_2, 1, 2);


        // When
        final Iterable<? extends Element> results = federatedStore.execute(new GetElements.Builder()
                .input(new EntitySeed(SOURCE_BASIC))
                .view(new View.Builder()
                        .edge(GROUP_BASIC_EDGE, new ViewElementDefinition.Builder()
                                .groupBy()
                                .build())
                        .build())
                .build(), testContext);

        // Then
        final HashSet<Edge> expected = new HashSet<>();
        // Graph a, element 1: prop1 present, prop2 missing
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, VALUE_1)
                .build());
        // Graph a, element 2: prop1 present, prop2 missing
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_2)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, VALUE_1)
                .build());
        // Graph b, element 1: prop1 present, prop2 present
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, VALUE_1)
                .property(PROPERTY_2, VALUE_2)
                .build());
        // Graph b, element 2: prop1 present, prop2 present
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_2)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, VALUE_1)
                .property(PROPERTY_2, VALUE_2)
                .build());

        assertThat((Iterable<Edge>) results)
                .isNotNull()
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    private void addGroupCollisionGraphs() throws OperationException {
        addGraphWith(GRAPH_ID_A, STRING_TYPE, PROPERTY_1);
        addGraphWith(GRAPH_ID_B, STRING_TYPE, PROPERTY_2);
    }

    private void addOverlappingPropertiesGraphs(final Schema stringSchema) throws OperationException {
        addGraphWith(GRAPH_ID_A, stringSchema, PROPERTY_1);
        addGraphWith(GRAPH_ID_B, stringSchema, PROPERTY_1, PROPERTY_2);
    }

    private void addGraphWith(final String graphId, final Schema stringType, final String... property) throws OperationException {
        federatedStore.execute(new AddGraph.Builder()
                .graphId(graphId)
                .schema(new Schema.Builder()
                        .edge(GROUP_BASIC_EDGE, new SchemaEdgeDefinition.Builder()
                                .source(STRING)
                                .destination(STRING)
                                .directed(DIRECTED_EITHER)
                                .properties(Arrays.stream(property).collect(Collectors.toMap(p -> p, p -> STRING)))
                                .build())
                        .type(DIRECTED_EITHER, Boolean.class)
                        .merge(stringType)
                        .build())
                .storeProperties(STORE_PROPERTIES.clone())
                .build(), testContext);
    }

    private void addEdgeBasicWith(final String destination, final Integer... propertyValues) throws OperationException {
        federatedStore.execute(new AddElements.Builder()
                .input(edgeBasicWith(destination, propertyValues))
                .build(), testContext);
    }

    private Edge edgeBasicWith(final String destination, final Integer... propertyValues) {
        return new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(destination)
                .properties(Arrays.stream(propertyValues).collect(Collectors.toMap(FederatedStoreTestUtil::property, FederatedStoreTestUtil::value))).build();
    }
}
