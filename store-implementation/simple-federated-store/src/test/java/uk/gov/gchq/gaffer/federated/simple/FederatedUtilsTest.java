/*
 * Copyright 2024 Crown Copyright
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
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.StoreProperties;
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
        View testView = new View.Builder()
            .entity(entityInSchema)
            .entity("entityNotInSchema")
            .edge(edgeInSchema)
            .edge("edgeNotInSchema")
            .build();
        GraphSerialisable graph = new GraphSerialisable(
            new GraphConfig("test"),
            new Schema.Builder()
                .entity(entityInSchema, new SchemaEntityDefinition())
                .edge(edgeInSchema, new SchemaEdgeDefinition()).build(),
            new StoreProperties());

        // When
        View fixedView =  FederatedUtils.getValidViewForGraph(testView, graph);

        // Then
        assertThat(fixedView.getEntityGroups()).containsOnly(entityInSchema);
        assertThat(fixedView.getEdgeGroups()).containsOnly(edgeInSchema);
    }

    @Test
    void shouldPreventExecutionIfNoGroupsInViewAreRelevant() {
        // Given
        View testView = new View.Builder()
                .entity("entityNotInSchema")
                .edge("edgeNotInSchema")
                .build();
        GraphSerialisable graph = new GraphSerialisable(
                new GraphConfig("test"),
                new Schema.Builder()
                        .entity("entityInSchema", new SchemaEntityDefinition())
                        .edge("edgeInSchema", new SchemaEdgeDefinition()).build(),
                new StoreProperties());

        // When
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> FederatedUtils.getValidViewForGraph(testView, graph));
    }

    @Test
    void shouldChangeViewsOfNestedOperations() {
        String entityInSchema = "entityInSchema";
        String edgeInSchema = "edgeInSchema";
        GraphSerialisable graph = new GraphSerialisable(
            new GraphConfig("test"),
            new Schema.Builder()
                .entity(entityInSchema, new SchemaEntityDefinition())
                .edge(edgeInSchema, new SchemaEdgeDefinition()).build(),
            new StoreProperties());

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
            .filter(op -> op instanceof OperationView)
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

        GraphSerialisable graph1 = new GraphSerialisable(
                new GraphConfig("graph1"),
                new Schema.Builder().entity(sharedGroup, new SchemaEntityDefinition()).build(),
                new StoreProperties());
        GraphSerialisable graph2 = new GraphSerialisable(
                new GraphConfig("graph2"),
                new Schema.Builder().entity(sharedGroup, new SchemaEntityDefinition()).build(),
                new StoreProperties());
        GraphSerialisable graph3 = new GraphSerialisable(
                new GraphConfig("graph3"),
                new Schema.Builder().entity("notShared", new SchemaEntityDefinition()).build(),
                new StoreProperties());

        // When/Then
        assertThat(FederatedUtils.doGraphsShareGroups(Arrays.asList(graph1, graph2))).isTrue();
        assertThat(FederatedUtils.doGraphsShareGroups(Arrays.asList(graph1, graph3))).isFalse();
    }

}
