/*
 * Copyright 2022-2023 Crown Copyright
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
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.serialisation.implementation.MultiSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedIntegerSerialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.Last;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_A;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_B;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_EDGE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.INTEGER;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextTestUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_TRUE;
import static uk.gov.gchq.gaffer.store.TestTypes.STRING_TYPE;
import static uk.gov.gchq.gaffer.store.TestTypes.VISIBILITY;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedStoreSchemaOverlapTest {
    public static final AccumuloProperties STORE_PROPERTIES = loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);

    private static final TypeDefinition INTEGER_TYPE = new TypeDefinition.Builder()
            .clazz(Integer.class)
            .serialiser(new OrderedIntegerSerialiser())
            .aggregateFunction(new Sum())
            .validateFunctions(new IsLessThan(10))
            .build();

    private static final SchemaEdgeDefinition EDGE_DEFINITION = new SchemaEdgeDefinition.Builder()
            .source(STRING)
            .destination(STRING)
            .description(GRAPH_ID_A)
            .directed(DIRECTED_EITHER)
            .groupBy(PROPERTY_1)
            .property(PROPERTY_1, INTEGER)
            .property(VISIBILITY, STRING)
            .build();

    private static final Schema GRAPH_A_SCHEMA = new Schema.Builder()
            .edge(GROUP_BASIC_EDGE, EDGE_DEFINITION)
            .visibilityProperty(VISIBILITY)
            .vertexSerialiser(new StringSerialiser())
            .type(STRING, STRING_TYPE)
            .type(INTEGER, INTEGER_TYPE)
            .type(DIRECTED_EITHER, Boolean.class)
            .type(DIRECTED_TRUE, Boolean.class)
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
    public void shouldMergeSameSchema() throws OperationException {
        // Given
        addGraphWith(GRAPH_ID_A, GRAPH_A_SCHEMA);
        addGraphWith(GRAPH_ID_B, GRAPH_A_SCHEMA);

        // When
        final Schema schema = federatedStore.execute(new GetSchema.Builder().build(), testContext);

        // Then
        assertThat(schema.validate().isValid())
                .withFailMessage(schema.validate().getErrorString())
                .isTrue();
        assertThat(schema).isEqualTo(GRAPH_A_SCHEMA);
    }

    @Test
    public void shouldSetVertexSerialiserToNullWhenConflictingSchemasMerge() throws OperationException {
        // Given
        addGraphWith(GRAPH_ID_A, GRAPH_A_SCHEMA);
        final Schema graphBSchema = new Schema.Builder()
                .merge(GRAPH_A_SCHEMA)
                .vertexSerialiser(new MultiSerialiser())
                .build();
        addGraphWith(GRAPH_ID_B, graphBSchema);

        // When
        final Schema schema = federatedStore.execute(new GetSchema.Builder().build(), testContext);

        // Then
        assertThat(schema.validate().isValid())
                .withFailMessage(schema.validate().getErrorString())
                .isTrue();
        assertThat(schema.getVertexSerialiser()).isNull();
        assertThat(schema.toString())
                .isNotEqualTo(GRAPH_A_SCHEMA.toString())
                .isNotEqualTo(graphBSchema.toString());
    }

    @Test
    public void shouldMergeSchemaWithoutTheClashingVisibilityProperty() throws OperationException {
        // Given
        addGraphWith(GRAPH_ID_A, GRAPH_A_SCHEMA);
        final String visibility1 = "visibility1";
        final SchemaEdgeDefinition edgeDef = new SchemaEdgeDefinition.Builder(EDGE_DEFINITION)
                .property(visibility1, STRING)
                .build();
        final Schema graphBSchema = new Schema.Builder()
                .merge(GRAPH_A_SCHEMA)
                .visibilityProperty(visibility1)
                .edge(GROUP_BASIC_EDGE, edgeDef)
                .build();
        addGraphWith(GRAPH_ID_B, graphBSchema);

        // When
        final Schema schema = federatedStore.execute(new GetSchema.Builder().build(), testContext);

        // Then
        assertThat(schema.validate().isValid())
                .withFailMessage(schema.validate().getErrorString())
                .isTrue();
        assertThat(schema.getVisibilityProperty())
                .withFailMessage("After a recovery from a collision with Visibility Property, the property should be set to Null")
                .isNull();
        assertThat(schema.toString())
                .isEqualTo(new Schema.Builder(GRAPH_A_SCHEMA)
                        .visibilityProperty(null)
                        .edge(GROUP_BASIC_EDGE, edgeDef)
                        .build()
                        .toString());
    }

    @Test
    public void shouldErrorWhenConflictingSchemasMergeAtTypeDefinitionClassClash() throws OperationException {
        // Given
        addGraphWith(GRAPH_ID_A, GRAPH_A_SCHEMA);
        final Schema graphBSchema = new Schema.Builder()
                .merge(GRAPH_A_SCHEMA)
                .type(INTEGER, STRING_TYPE)
                .build();
        addGraphWith(GRAPH_ID_B, graphBSchema);

        // When then
        assertThatException()
                .isThrownBy(() -> federatedStore.execute(new GetSchema.Builder().build(), testContext))
                .withStackTraceContaining("MergeSchema function unable to recover from error")
                .withStackTraceContaining("Error with the schema type named:integer")
                .withStackTraceContaining("conflict with the type class, options are: java.lang.String and java.lang.Integer");
    }

    @Test
    public void shouldRetainGroupSourceTypeWhenConflictingSchemasMerge() throws OperationException {
        // Given
        addGraphWith(GRAPH_ID_A, GRAPH_A_SCHEMA);
        final Schema graphBSchema = new Schema.Builder()
                .merge(GRAPH_A_SCHEMA)
                .edge(GROUP_BASIC_EDGE, new SchemaEdgeDefinition.Builder()
                        .merge(EDGE_DEFINITION)
                        .source(INTEGER)
                        .build())
                .build();
        addGraphWith(GRAPH_ID_B, graphBSchema);

        // When
        final Schema schema = federatedStore.execute(new GetSchema.Builder().build(), testContext);

        // Then
        assertThat(schema.validate().isValid())
                .withFailMessage(schema.validate().getErrorString())
                .isTrue();
        assertThat(schema.getEdge(GROUP_BASIC_EDGE).getSource()).isEqualTo(STRING);
        assertThat(schema.toString())
                .isEqualTo(GRAPH_A_SCHEMA.toString())
                .isNotEqualTo(graphBSchema.toString());
    }

    @Test
    public void shouldRetainGroupDestinationTypeWhenConflictingSchemasMerge() throws OperationException {
        // Given
        addGraphWith(GRAPH_ID_A, GRAPH_A_SCHEMA);
        final Schema graphBSchema = new Schema.Builder()
                .merge(GRAPH_A_SCHEMA)
                .edge(GROUP_BASIC_EDGE, new SchemaEdgeDefinition.Builder()
                        .merge(EDGE_DEFINITION)
                        .destination(INTEGER)
                        .build())
                .build();
        addGraphWith(GRAPH_ID_B, graphBSchema);

        // When
        final Schema schema = federatedStore.execute(new GetSchema.Builder().build(), testContext);

        // Then
        assertThat(schema.validate().isValid())
                .withFailMessage(schema.validate().getErrorString())
                .isTrue();
        assertThat(schema.getEdge(GROUP_BASIC_EDGE).getDestination()).isEqualTo(STRING);
        assertThat(schema.toString())
                .isEqualTo(GRAPH_A_SCHEMA.toString())
                .isNotEqualTo(graphBSchema.toString());
    }

    @Test
    public void shouldRetainGroupDescriptionWhenConflictingSchemasMerge() throws OperationException {
        // Given
        addGraphWith(GRAPH_ID_A, GRAPH_A_SCHEMA);
        final Schema graphBSchema = new Schema.Builder()
                .merge(GRAPH_A_SCHEMA)
                .edge(GROUP_BASIC_EDGE, new SchemaEdgeDefinition.Builder()
                        .merge(EDGE_DEFINITION)
                        .description(GRAPH_ID_B)
                        .build())
                .build();
        addGraphWith(GRAPH_ID_B, graphBSchema);

        // When
        final Schema schema = federatedStore.execute(new GetSchema.Builder().build(), testContext);

        // Then
        assertThat(schema.validate().isValid())
                .withFailMessage(schema.validate().getErrorString())
                .isTrue();
        assertThat(schema.getEdge(GROUP_BASIC_EDGE).getDescription()).isEqualTo(GRAPH_ID_A);
        assertThat(schema.toString())
                .isEqualTo(GRAPH_A_SCHEMA.toString())
                .isNotEqualTo(graphBSchema.toString());
    }

    @Test
    public void shouldRetainGroupDirectedTypeWhenConflictingSchemasMerge() throws OperationException {
        // Given
        addGraphWith(GRAPH_ID_A, GRAPH_A_SCHEMA);
        final Schema graphBSchema = new Schema.Builder()
                .merge(GRAPH_A_SCHEMA)
                .edge(GROUP_BASIC_EDGE, new SchemaEdgeDefinition.Builder()
                        .merge(EDGE_DEFINITION)
                        .directed(DIRECTED_TRUE)
                        .build())
                .build();
        addGraphWith(GRAPH_ID_B, graphBSchema);

        // When
        final Schema schema = federatedStore.execute(new GetSchema.Builder().build(), testContext);

        // Then
        assertThat(schema.validate().isValid())
                .withFailMessage(schema.validate().getErrorString())
                .isTrue();
        assertThat(schema.getEdge(GROUP_BASIC_EDGE).getDirected()).isEqualTo(DIRECTED_EITHER);
        assertThat(schema.toString())
                .isEqualTo(GRAPH_A_SCHEMA.toString())
                .isNotEqualTo(graphBSchema.toString());
    }

    @Test
    public void shouldRetainGroupGroupByWhenConflictingSchemasMerge() throws OperationException {
        // Given
        addGraphWith(GRAPH_ID_A, GRAPH_A_SCHEMA);
        final Schema graphBSchema = new Schema.Builder()
                .merge(GRAPH_A_SCHEMA)
                .edge(GROUP_BASIC_EDGE, new SchemaEdgeDefinition.Builder()
                        .merge(EDGE_DEFINITION)
                        .groupBy(VISIBILITY)
                        .build())
                .build();
        addGraphWith(GRAPH_ID_B, graphBSchema);

        // When
        final Schema schema = federatedStore.execute(new GetSchema.Builder().build(), testContext);

        // Then
        assertThat(schema.validate().isValid())
                .withFailMessage(schema.validate().getErrorString())
                .isTrue();
        assertThat(schema.getEdge(GROUP_BASIC_EDGE).getGroupBy()).containsExactly(PROPERTY_1);
        assertThat(schema.toString())
                .isEqualTo(GRAPH_A_SCHEMA.toString())
                .isNotEqualTo(graphBSchema.toString());
    }

    @Test
    public void shouldErrorAtTypeDefinitionAggregationFunctionWhenConflictingDuringSchemasMerge() throws OperationException {
        // Given
        addGraphWith(GRAPH_ID_A, GRAPH_A_SCHEMA);
        final Schema graphBSchema = new Schema.Builder()
                .merge(GRAPH_A_SCHEMA)
                .type(INTEGER, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .serialiser(new OrderedIntegerSerialiser())
                        .aggregateFunction(new Last())
                        .validateFunctions(new IsLessThan(10))
                        .build())
                .build();
        addGraphWith(GRAPH_ID_B, graphBSchema);

        // When
        assertThatException()
                .isThrownBy(() -> federatedStore.execute(new GetSchema.Builder().build(), testContext))
                .withStackTraceContaining("Error with the schema type named:integer")
                .withStackTraceContaining("Unable to merge schemas because of conflict with the aggregate function")
                .withStackTraceContaining("options are: uk.gov.gchq.koryphe.impl.binaryoperator.Last")
                .withStackTraceContaining("and uk.gov.gchq.koryphe.impl.binaryoperator.Sum");
    }

    @Test
    public void shouldAppendTypeDefinitionValidationFunctionWhenConflictingSchemasMerge() throws OperationException {
        // Given
        addGraphWith(GRAPH_ID_A, GRAPH_A_SCHEMA);
        final Schema graphBSchema = new Schema.Builder()
                .merge(GRAPH_A_SCHEMA)
                .type(INTEGER, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .serialiser(new OrderedIntegerSerialiser())
                        .aggregateFunction(new Sum())
                        .validateFunctions(new IsLessThan(20))
                        .build())
                .build();
        addGraphWith(GRAPH_ID_B, graphBSchema);

        // When
        final Schema schema = federatedStore.execute(new GetSchema.Builder().build(), testContext);

        // Then
        assertThat(schema.validate().isValid())
                .withFailMessage(schema.validate().getErrorString())
                .isTrue();
        assertThat(schema.getEdge(GROUP_BASIC_EDGE).getPropertyTypeDef(PROPERTY_1).getValidateFunctions())
                .containsExactlyInAnyOrder(new IsLessThan(10), new IsLessThan(20));
        assertThat(schema.toString())
                .isNotEqualTo(GRAPH_A_SCHEMA.toString())
                .isNotEqualTo(graphBSchema.toString());
    }

    private void addGraphWith(final String graphId, final Schema schema) throws OperationException {
        federatedStore.execute(new AddGraph.Builder()
                .graphId(graphId)
                .schema(schema)
                .storeProperties(STORE_PROPERTIES.clone())
                .build(), testContext);
    }
}
