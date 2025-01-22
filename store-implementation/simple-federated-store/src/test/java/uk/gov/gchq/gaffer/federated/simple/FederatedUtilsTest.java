/*
 * Copyright 2024-2025 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class FederatedUtilsTest {

    @Test
    void shouldRemoveGroupFromViewIfNotInSchema() {
        // Given
        String entityInSchema = "entityInSchema";
        String edgeInSchema = "edgeInSchema";
        String stringTypeName = "string";
        SchemaEntityDefinition entityDef = new SchemaEntityDefinition.Builder()
                .vertex(stringTypeName)
                .build();
        View testView = new View.Builder()
            .entity(entityInSchema)
            .entity("entityNotInSchema")
            .edge(edgeInSchema)
            .edge("edgeNotInSchema")
            .build();
        Graph graph = new Graph.Builder()
            .config(new GraphConfig("test"))
            .addSchema(new Schema.Builder()
                .type(stringTypeName, String.class)
                .entity(entityInSchema, entityDef)
                .edge(edgeInSchema, new SchemaEdgeDefinition.Builder()
                    .source(stringTypeName)
                    .destination(stringTypeName)
                    .build()).build())
            .storeProperties(new MapStoreProperties())
            .build();
        // When
        View fixedView =  FederatedUtils.getValidViewForGraph(testView, graph);

        // Then
        assertThat(fixedView.getEntityGroups()).containsOnly(entityInSchema);
        assertThat(fixedView.getEdgeGroups()).containsOnly(edgeInSchema);
    }

    @Test
    void shouldPreventExecutionIfNoGroupsInViewAreRelevant() {
        // Given
        String stringTypeName = "string";
        SchemaEntityDefinition entityDef = new SchemaEntityDefinition.Builder()
                .vertex(stringTypeName)
                .build();
        View testView = new View.Builder()
                .entity("entityNotInSchema")
                .edge("edgeNotInSchema")
                .build();
        Graph graph = new Graph.Builder()
            .config(new GraphConfig("test"))
            .addSchema(new Schema.Builder()
                .type(stringTypeName, String.class)
                .entity("entityInSchema", entityDef)
                .edge("edgeInSchema", new SchemaEdgeDefinition.Builder()
                    .source(stringTypeName)
                    .destination(stringTypeName)
                    .build()).build())
            .storeProperties(new MapStoreProperties())
            .build();

        // When
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> FederatedUtils.getValidViewForGraph(testView, graph));
    }

    @Test
    void shouldChangeViewsOfNestedOperations() {
        String entityInSchema = "entityInSchema";
        String edgeInSchema = "edgeInSchema";
        String stringTypeName = "string";
        SchemaEntityDefinition entityDef = new SchemaEntityDefinition.Builder()
            .vertex(stringTypeName)
            .build();
        Graph graph = new Graph.Builder()
            .config(new GraphConfig("test"))
            .addSchema(new Schema.Builder()
                .type(stringTypeName, String.class)
                .entity(entityInSchema, entityDef)
                .edge(edgeInSchema, new SchemaEdgeDefinition.Builder()
                    .source(stringTypeName)
                    .destination(stringTypeName)
                    .build()).build())
            .storeProperties(new MapStoreProperties())
            .build();

        // View with some mixed groups in
        View testView = new View.Builder()
            .entity(entityInSchema)
            .entity("entityNotInSchema")
            .edge(edgeInSchema)
            .edge("edgeNotInSchema")
            .build();

        // Build nested operation chain with multiple views
        OperationChain<?> nestedViewChain = new OperationChain.Builder()
            .first(new GetAllElements.Builder().view(testView).build())
            .then(new OperationChain.Builder()
                .first(new GetAllElements.Builder().view(testView).build())
                .then(new OperationChain.Builder()
                    .first(new GetAllElements.Builder().view(testView).build())
                    .build())
                .build())
            .build();

        // Get a fixed operation chain
        List<Operation> newChain = FederatedUtils.getValidOperationForGraph(nestedViewChain, graph, 0, 5).flatten();
        List<OperationView> fixedOperations = newChain.stream()
            .filter(OperationView.class::isInstance)
            .map(op -> (OperationView) op)
            .collect(Collectors.toList());

        // Check all the views only contain relevant groups
        fixedOperations.stream()
            .map(OperationView::getView)
            .forEach(v -> {
                assertThat(v.getEntityGroups()).containsOnly(entityInSchema);
                assertThat(v.getEdgeGroups()).containsOnly(edgeInSchema);
            });
    }

    @Test
    void shouldDetectIfGraphsShareGroups() {
        // Given
        String sharedGroup = "sharedGroup";
        String stringTypeName = "string";
        SchemaEntityDefinition entityDef = new SchemaEntityDefinition.Builder()
                .vertex(stringTypeName)
                .build();
        Graph graph1 = new Graph.Builder()
            .config(new GraphConfig("graph1"))
            .addSchema(new Schema.Builder()
                .type(stringTypeName, String.class)
                .entity(sharedGroup, entityDef).build())
            .storeProperties(new MapStoreProperties())
            .build();
        Graph graph2 = new Graph.Builder()
            .config(new GraphConfig("graph2"))
            .addSchema(new Schema.Builder()
                .type(stringTypeName, String.class)
                .entity(sharedGroup, entityDef).build())
            .storeProperties(new MapStoreProperties())
            .build();
        Graph graph3 = new Graph.Builder()
            .config(new GraphConfig("graph3"))
            .addSchema(new Schema.Builder()
                .type(stringTypeName, String.class)
                .entity("notShared", entityDef).build())
            .storeProperties(new MapStoreProperties())
            .build();

        // When/Then
        assertThat(FederatedUtils.doGraphsShareGroups(Arrays.asList(graph1, graph2))).isTrue();
        assertThat(FederatedUtils.doGraphsShareGroups(Arrays.asList(graph1, graph3))).isFalse();
    }

}
