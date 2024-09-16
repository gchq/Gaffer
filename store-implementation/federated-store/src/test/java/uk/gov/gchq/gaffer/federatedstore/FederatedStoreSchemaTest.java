/*
 * Copyright 2017-2024 Crown Copyright
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

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.NoAccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.UnrestrictedAccessPredicate;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
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
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static uk.gov.gchq.gaffer.data.util.ElementUtil.assertElementEqualsIncludingMatchedVertex;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.DEST_BASIC;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_A;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_B;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_C;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_EDGE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_2;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SOURCE_BASIC;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.VALUE_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.VALUE_2;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextTestUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.getFederatedStorePropertiesWithHashMapCache;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.property;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getDefaultMergeFunction;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedOperation;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;
import static uk.gov.gchq.gaffer.user.StoreUser.AUTH_1;
import static uk.gov.gchq.gaffer.user.StoreUser.authUser;
import static uk.gov.gchq.gaffer.user.StoreUser.blankUser;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedStoreSchemaTest {
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
        federatedStore.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, getFederatedStorePropertiesWithHashMapCache());

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
        assertThat(graphIds).containsExactlyInAnyOrder(GRAPH_ID_A, GRAPH_ID_B, GRAPH_ID_C);
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
    public void shouldBeAbleToGetElementsWithOverlappingSchemasUsingDefaultMergeFunction() throws OperationException {
        // Given
        addOverlappingPropertiesGraphs(STRING_TYPE);

        // Element 1
        addEdgeBasicWith(DEST_BASIC, 1);

        // Element 2
        addEdgeBasicWith(DEST_2, 1, 2);

        // When
        // getDefaultMergeFunction specified - behaves like pre 2.0 aggregation
        final Iterable<? extends Element> results = (Iterable<? extends Element>) federatedStore.execute(new FederatedOperation.Builder()
                .op(new GetElements.Builder()
                        .input(new EntitySeed(SOURCE_BASIC))
                        .view(new View.Builder()
                                .edge(GROUP_BASIC_EDGE, new ViewElementDefinition.Builder()
                                        .properties(PROPERTY_2)
                                        .build())
                                .build())
                        .build())
                .mergeFunction(getDefaultMergeFunction())
                .build(), testContext);

        // Then
        final HashSet<Edge> expected = new HashSet<>();
        // Graph a, element 1: prop2 missing
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .build());
        // Graph a, element 2: prop2 missing
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_2)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .build());
        // Graph b, element 1: prop2 empty (see below)
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                // Due to a string serialisation quirk, missing properties (null value)
                // are deserialised as empty strings
                .property(PROPERTY_2, "")
                .build());
        // Graph b, element 2: prop2 present
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_2)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_2, VALUE_2)
                .build());

        assertElementEqualsIncludingMatchedVertex(expected, results);
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
        // No merge function specified - ApplyViewToElementsFunction is used
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
        // Element 1: only 1 copy of prop2 empty (see below)
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                // Due to a string serialisation quirk, missing properties (null value)
                // are deserialised as empty strings
                .property(PROPERTY_2, "")
                .build());
        // Element 2: only 1 copy of prop2 present
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_2)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_2, VALUE_2)
                .build());

        assertElementEqualsIncludingMatchedVertex(expected, results);
    }

    @Test
    public void shouldBeAbleToGetSchemaWithOverlappingSchemas() throws OperationException {
        // Given
        addOverlappingPropertiesGraphs(STRING_TYPE);

        // When
        final Schema schema = federatedStore.execute(new GetSchema.Builder().build(), testContext);

        // Then
        assertThat(schema.validate().isValid())
                .withFailMessage(schema.validate().getErrorString())
                .isTrue();
    }

    @Test
    public void shouldGetSchemaWithOperationAndMethodWithContext() throws OperationException {
        // Given
        addGraphWith(GRAPH_ID_A, STRING_TYPE, PROPERTY_1);

        // When
        final Schema schemaFromOperation = federatedStore.execute(new GetSchema.Builder().build(), testContext);
        final Schema schemaFromStore = federatedStore.getSchema(testContext, false);

        // Then
        assertThat(schemaFromOperation).isEqualTo(schemaFromStore);
    }

    @Test
    public void shouldGetBlankSchemaWhenUsingDefaultMethod() throws OperationException {
        // Given
        addGraphWith(GRAPH_ID_A, STRING_TYPE, PROPERTY_1);

        // When
        final Schema schemaFromStoreMethod = federatedStore.getOriginalSchema(); // No Context, results in blank schema returned

        // Then
        assertThat(schemaFromStoreMethod).isEqualTo(new Schema());
    }

    @Test
    public void shouldGetSchemaWhenUsingDefaultMethodWhenPermissiveReadAccessPredicateConfigured() throws OperationException {
        // Given
        addGraphWithContextAndAccess(GRAPH_ID_A, STRING_TYPE, GROUP_BASIC_EDGE, testContext, new UnrestrictedAccessPredicate(), PROPERTY_1);

        // When
        final Schema schemaFromStoreMethod = federatedStore.getOriginalSchema();

        // Then
        assertThat(schemaFromStoreMethod.getEdge(GROUP_BASIC_EDGE).getProperties()).contains(PROPERTY_1);
    }

    @Test
    public void shouldErrorWhenGetSchemaASharedGroupHasNoSharedProperties() throws OperationException {
        // Given
        addGraphWith(GRAPH_ID_A, STRING_TYPE, PROPERTY_1);

        // When
        final Schema schemaA = federatedStore.getSchema(testContext, false);

        // Then
        assertThat(schemaA.getTypes().size()).isEqualTo(2);
        assertThat(schemaA.getType(STRING).getClazz()).isEqualTo(String.class);
        assertThat(schemaA.getEdge(GROUP_BASIC_EDGE).getProperties().size()).isEqualTo(1);

        // Given
        addGraphWith(GRAPH_ID_B, STRING_REQUIRED_TYPE, PROPERTY_2);

        // When
        assertThatException()
                .isThrownBy(() -> federatedStore.getSchema(testContext, false))
                .withStackTraceContaining("MergeSchema function unable to recover from error")
                .withStackTraceContaining("Element group properties cannot be defined in different schema parts, they must all be defined in a single schema part")
                .withStackTraceContaining("Please fix this group: BasicEdge");
    }

    @Test
    public void shouldChangeReturnSchemaWhenAddingGraphWithOverLapProperty() throws OperationException {
        // Given
        addGraphWith(GRAPH_ID_A, STRING_TYPE, PROPERTY_1);

        // When
        final Schema schemaA = federatedStore.getSchema(testContext, false);

        // Then
        assertThat(schemaA.getTypes().size()).isEqualTo(2);
        assertThat(schemaA.getType(STRING).getClazz()).isEqualTo(String.class);
        assertThat(schemaA.getEdge(GROUP_BASIC_EDGE).getProperties().size()).isEqualTo(1);

        // Given
        addGraphWith(GRAPH_ID_B, STRING_REQUIRED_TYPE, PROPERTY_1, PROPERTY_2);

        // When
        final Schema schemaAB = federatedStore.getSchema(testContext, false);

        // Then
        assertThat(schemaAB).isNotEqualTo(schemaA);
        assertThat(schemaAB.getEdge(GROUP_BASIC_EDGE).getProperties())
                .contains(PROPERTY_1)
                .contains(PROPERTY_2);
    }

    @Test
    public void shouldGetSchemaForOwningUser() throws OperationException {
        // Given
        addGraphWith(GRAPH_ID_A, STRING_REQUIRED_TYPE, PROPERTY_1);
        addGraphWithContextAndAuths(GRAPH_ID_B, STRING_TYPE, "hidden" + GROUP_BASIC_EDGE, singleton(AUTH_1), new Context(authUser()), PROPERTY_2);

        // When
        final Schema schemaFromOwningUser = federatedStore.getSchema(testContext, false);

        // Then
        assertThat(schemaFromOwningUser.getEdge("hidden" + GROUP_BASIC_EDGE)).withFailMessage("Revealing hidden schema").isNull();
        assertThat(schemaFromOwningUser.getEdge(GROUP_BASIC_EDGE).getProperties()).contains(PROPERTY_1);
    }

    @Test
    public void shouldNotGetSchemaForOwningUserWhenBlockingReadAccessPredicateConfigured() throws OperationException {
        // Given
        addGraphWithContextAndAccess(GRAPH_ID_A, STRING_TYPE, GROUP_BASIC_EDGE, testContext, new NoAccessPredicate(), PROPERTY_1);

        // When
        final Schema schemaFromOwningUser = federatedStore.getSchema(testContext, false);

        // Then
        assertThat(schemaFromOwningUser).withFailMessage("Revealing blocked schema, should be empty").isEqualTo(new Schema());
    }

    @Test
    public void shouldGetSchemaForAuthUser() throws OperationException {
        // Given
        final User authUser = new User.Builder().userId("authUser2").opAuths(AUTH_1).build();
        addGraphWithContextAndAuths(GRAPH_ID_B, STRING_TYPE, GROUP_BASIC_EDGE, singleton(AUTH_1), new Context(authUser()), PROPERTY_1);

        // When
        final Schema schemaFromAuthUser = federatedStore.getSchema(new Context(authUser), false);
        final Schema schemaFromTestUser = federatedStore.getSchema(testContext, false);

        // Then
        assertThat(schemaFromTestUser.getEdge("hidden" + GROUP_BASIC_EDGE)).withFailMessage("Revealing hidden schema").isNull();
        assertThat(schemaFromTestUser).withFailMessage("Revealing hidden schema, should be empty").isEqualTo(new Schema());
        assertThat(schemaFromAuthUser.getEdge(GROUP_BASIC_EDGE).getProperties()).contains(PROPERTY_1);
    }

    @Test
    public void shouldNotGetSchemaForBlankUser() throws OperationException {
        // Given
        addGraphWith(GRAPH_ID_A, STRING_REQUIRED_TYPE, PROPERTY_1);

        // When
        final Schema schemaFromBlankUser = federatedStore.getSchema(new Context(blankUser()), false);

        // Then
        assertThat(schemaFromBlankUser).withFailMessage("Revealing schema to blank user, should be empty").isEqualTo(new Schema());
    }

    @Test
    public void shouldGetSchemaForBlankUserWhenPermissiveReadAccessPredicateConfigured() throws OperationException {
        // Given
        addGraphWithContextAndAccess(GRAPH_ID_A, STRING_TYPE, GROUP_BASIC_EDGE, testContext, new UnrestrictedAccessPredicate(), PROPERTY_1);

        // When
        final Schema schemaFromBlankUser = federatedStore.getSchema(new Context(blankUser()), false);

        // Then
        assertThat(schemaFromBlankUser.getEdge(GROUP_BASIC_EDGE).getProperties()).contains(PROPERTY_1);
    }

    @Test
    public void shouldValidateCorrectlyWithOverlappingSchemasUsingDefaultMergeFunction() throws OperationException {
        // Given
        addOverlappingPropertiesGraphs(STRING_REQUIRED_TYPE);

        // When
        addEdgeBasicWith(DEST_2, 1, 2);

        // getDefaultMergeFunction specified - behaves like pre 2.0 aggregation
        final Iterable<? extends Element> results = (Iterable<? extends Element>) federatedStore.execute(new FederatedOperation.Builder()
                .op(new GetAllElements())
                .mergeFunction(getDefaultMergeFunction())
                .build(), testContext);

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
    public void shouldValidateCorrectlyWithOverlappingSchemas() throws OperationException {
        // Given
        addOverlappingPropertiesGraphs(STRING_REQUIRED_TYPE);

        // When
        addEdgeBasicWith(DEST_2, 1, 2);

        // No merge function specified - ApplyViewToElementsFunction is used
        // An exception is raised because the aggregated results are missing a validated property
        assertThatExceptionOfType(OperationException.class)
                .isThrownBy(() -> {
                    federatedStore.execute(new GetAllElements.Builder().build(), testContext);
                })
                .withStackTraceContaining("returned false for properties: {%s: null}", property(2));
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
    public void shouldBeAbleToIngestAggregateWithOverlappingSchemasUsingDefaultMergeFunction() throws OperationException {
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
        // getDefaultMergeFunction specified - behaves like pre 2.0 aggregation
        final Iterable<? extends Element> results = (Iterable<? extends Element>) federatedStore.execute(new FederatedOperation.Builder()
                .op(new GetElements.Builder()
                        .input(new EntitySeed(SOURCE_BASIC))
                        .view(new View.Builder()
                                .edge(GROUP_BASIC_EDGE, new ViewElementDefinition.Builder().build()).build())
                        .build())
                .mergeFunction(getDefaultMergeFunction())
                .build(), testContext);

        // Then
        final HashSet<Edge> expected = new HashSet<>();
        // Graph a: prop1 aggregated, prop2 missing
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, "value1,value1")
                .build());
        // Graph b: prop1 aggregated, prop2 aggregated
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, "value1,value1")
                .property(PROPERTY_2, "value2,value2")
                .build());

        assertElementEqualsIncludingMatchedVertex(expected, results);
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
        // No merge function specified - ApplyViewToElementsFunction is used
        final Iterable<? extends Element> results = federatedStore.execute(new GetElements.Builder()
                .input(new EntitySeed(SOURCE_BASIC))
                .view(new View.Builder()
                        .edge(GROUP_BASIC_EDGE, new ViewElementDefinition.Builder().build()).build())
                .build(), testContext);

        // Then
        final HashSet<Edge> expected = new HashSet<>();
        // prop1 aggregated: (value1,value1) from both graphs
        // prop2 aggregated: (value2,value2) from graph b
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, "value1,value1,value1,value1")
                .property(PROPERTY_2, "value2,value2")
                .build());

        assertElementEqualsIncludingMatchedVertex(expected, results);
    }

    @Test
    public void shouldBeAbleToIngestAggregateMissingPropertyWithOverlappingSchemasUsingDefaultMergeFunction() throws OperationException {
        // Given
        addOverlappingPropertiesGraphs(STRING_TYPE);

        // Element 1
        addEdgeBasicWith(DEST_BASIC, 1);

        // Element 2
        addEdgeBasicWith(DEST_BASIC, 1);

        // When
        // getDefaultMergeFunction specified - behaves like pre 2.0 aggregation
        final Iterable<? extends Element> results = (Iterable<? extends Element>) federatedStore.execute(new FederatedOperation.Builder()
                .op(new GetElements.Builder()
                        .input(new EntitySeed(SOURCE_BASIC))
                        .view(new View.Builder()
                                .edge(GROUP_BASIC_EDGE, new ViewElementDefinition.Builder().build()).build())
                        .build())
                .mergeFunction(getDefaultMergeFunction())
                .build(), testContext);

        // Then
        final HashSet<Edge> expected = new HashSet<>();
        // Graph a: prop1 aggregated, prop2 missing
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, "value1,value1")
                .build());
        // Graph b: prop1 aggregated, prop2 aggregated empty (see below)
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, "value1,value1")
                // Due to a string serialisation quirk, missing properties (null value)
                // are deserialised as empty strings, so here 2 empty strings are aggregated
                .property(PROPERTY_2, ",")
                .build());

        assertElementEqualsIncludingMatchedVertex(expected, results);
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
        // No merge function specified - ApplyViewToElementsFunction is used
        final Iterable<? extends Element> results = federatedStore.execute(new GetElements.Builder()
                .input(new EntitySeed(SOURCE_BASIC))
                .view(new View.Builder()
                        .edge(GROUP_BASIC_EDGE, new ViewElementDefinition.Builder().build()).build())
                .build(), testContext);

        // Then
        final HashSet<Edge> expected = new HashSet<>();
        // prop1 aggregated: (value1,value1) from both graphs
        // prop2 aggregated: 2 aggregated empty strings from graph b
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, "value1,value1,value1,value1")
                // Due to a string serialisation quirk, missing properties (null value)
                // are deserialised as empty strings, so here 2 empty strings are aggregated
                .property(PROPERTY_2, ",")
                .build());

        assertElementEqualsIncludingMatchedVertex(expected, results);
    }

    @Test
    public void shouldBeAbleToViewPropertyWithOverlappingSchemasUsingDefaultMergeFunction() throws OperationException {
        // Given
        addOverlappingPropertiesGraphs(STRING_TYPE);

        // Element 1
        addEdgeBasicWith(DEST_BASIC, 1, 2);

        // Element 2
        addEdgeBasicWith(DEST_2, 1, 2);


        // When
        // getDefaultMergeFunction specified - behaves like pre 2.0 aggregation
        final Iterable<? extends Element> results = (Iterable<? extends Element>) federatedStore.execute(new FederatedOperation.Builder()
                .op(new GetElements.Builder()
                        .input(new EntitySeed(SOURCE_BASIC))
                        .view(new View.Builder()
                                .edge(GROUP_BASIC_EDGE, new ViewElementDefinition.Builder()
                                        .properties(PROPERTY_2)
                                        .build())
                                .build())
                        .build())
                .mergeFunction(getDefaultMergeFunction())
                .build(), testContext);

        // Then
        final HashSet<Edge> expected = new HashSet<>();
        // Graph a, element 1: prop1 omitted, prop2 missing
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .build());
        // Graph a, element 2: prop1 omitted, prop2 missing
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_2)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .build());
        // Graph b, element 1: prop1 omitted, prop2 present
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_2, VALUE_2)
                .build());
        // Graph b, element 2: prop1 omitted, prop2 present
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_2)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_2, VALUE_2)
                .build());

        assertElementEqualsIncludingMatchedVertex(expected, results);
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
        // No merge function specified - ApplyViewToElementsFunction is used
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
        // Element 1: prop1 omitted, prop2 present
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_2, VALUE_2)
                .build());
        // Element 2: prop1 omitted, prop2 present
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_2)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_2, VALUE_2)
                .build());

        assertElementEqualsIncludingMatchedVertex(expected, results);
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
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, VALUE_1)
                .property(PROPERTY_2, VALUE_2)
                .build());
        // Graph b, element 2
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_2)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, VALUE_1)
                .property(PROPERTY_2, VALUE_2)
                .build());

        assertElementEqualsIncludingMatchedVertex(expected, results);
    }

    @Test
    public void shouldBeAbleToQueryAggregatePropertyWithOverlappingSchemasUsingDefaultMergeFunction() throws OperationException {
        // Given
        addOverlappingPropertiesGraphs(STRING_TYPE);

        // Element 1
        addEdgeBasicWith(DEST_BASIC, 1, 2);

        // Element 2
        addEdgeBasicWith(DEST_2, 1, 2);


        // When
        // getDefaultMergeFunction specified - behaves like pre 2.0 aggregation
        final Iterable<? extends Element> results = (Iterable<? extends Element>) federatedStore.execute(new FederatedOperation.Builder()
                .op(new GetElements.Builder()
                        .input(new EntitySeed(SOURCE_BASIC))
                        .view(new View.Builder()
                                .edge(GROUP_BASIC_EDGE, new ViewElementDefinition.Builder()
                                        .groupBy()
                                        .build())
                                .build())
                        .build())
                .mergeFunction(getDefaultMergeFunction())
                .build(), testContext);

        // Then
        final HashSet<Edge> expected = new HashSet<>();
        // Graph a, element 1: prop1 present, prop2 missing
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, VALUE_1)
                .build());
        // Graph a, element 2: prop1 present, prop2 missing
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_2)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, VALUE_1)
                .build());
        // Graph b, element 1: prop1 present, prop2 present
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, VALUE_1)
                .property(PROPERTY_2, VALUE_2)
                .build());
        // Graph b, element 2: prop1 present, prop2 present
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_2)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, VALUE_1)
                .property(PROPERTY_2, VALUE_2)
                .build());

        assertElementEqualsIncludingMatchedVertex(expected, results);
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
        // No merge function specified - ApplyViewToElementsFunction is used
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
        // Element 1: prop1 aggregated, prop2 present once
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_BASIC)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, "value1,value1")
                .property(PROPERTY_2, VALUE_2)
                .build());
        // Element 2: prop1 aggregated, prop2 present once
        expected.add(new Edge.Builder()
                .group(GROUP_BASIC_EDGE)
                .source(SOURCE_BASIC)
                .dest(DEST_2)
                .directed(true)
                .matchedVertex(EdgeId.MatchedVertex.SOURCE)
                .property(PROPERTY_1, "value1,value1")
                .property(PROPERTY_2, VALUE_2)
                .build());

        assertElementEqualsIncludingMatchedVertex(expected, results);
    }

    private void addGroupCollisionGraphs() throws OperationException {
        addGraphWith(GRAPH_ID_A, STRING_TYPE, PROPERTY_1);
        addGraphWith(GRAPH_ID_B, STRING_TYPE, PROPERTY_2);
    }

    private void addOverlappingPropertiesGraphs(final Schema stringSchema) throws OperationException {
        addGraphWith(GRAPH_ID_A, stringSchema, PROPERTY_1);
        addGraphWith(GRAPH_ID_B, stringSchema, PROPERTY_1, PROPERTY_2);
    }

    private AddGraph.Builder getAddGraphBuilder(final String graphId, final Schema stringType, final String edgeGroup, final String... property) {
        return new AddGraph.Builder()
                .graphId(graphId)
                .schema(new Schema.Builder()
                        .edge(edgeGroup, new SchemaEdgeDefinition.Builder()
                                .source(STRING)
                                .destination(STRING)
                                .directed(DIRECTED_EITHER)
                                .properties(Arrays.stream(property).collect(Collectors.toMap(p -> p, p -> STRING)))
                                .build())
                        .type(DIRECTED_EITHER, Boolean.class)
                        .merge(stringType)
                        .build())
                .storeProperties(STORE_PROPERTIES.clone());
    }

    private void addGraphWith(final String graphId, final Schema stringType, final String... property) throws OperationException {
        federatedStore.execute(getAddGraphBuilder(graphId, stringType, GROUP_BASIC_EDGE, property)
                .build(), testContext);
    }

    private void addGraphWithContextAndAuths(final String graphId, final Schema stringType, final String edgeGroup, Set<String> graphAuths,
                                             Context context, final String... property) throws OperationException {
        federatedStore.execute(getAddGraphBuilder(graphId, stringType, edgeGroup, property)
                .graphAuths(graphAuths.toArray(new String[0]))
                .build(), context);
    }

    private void addGraphWithContextAndAccess(final String graphId, final Schema stringType, final String edgeGroup, Context context,
                                              AccessPredicate read, final String... property) throws OperationException {
        federatedStore.execute(getAddGraphBuilder(graphId, stringType, edgeGroup, property)
                .readAccessPredicate(read)
                .build(), context);
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
                .directed(true)
                .properties(Arrays.stream(propertyValues).collect(Collectors.toMap(FederatedStoreTestUtil::property, FederatedStoreTestUtil::value))).build();
    }
}
