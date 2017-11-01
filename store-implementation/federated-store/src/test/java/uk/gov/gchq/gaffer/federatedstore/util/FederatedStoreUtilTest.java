/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.predicate.IsTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class FederatedStoreUtilTest {
    @Test
    public void shouldGetGraphIds() {
        // Given
        final Map<String, String> config = new HashMap<>();
        config.put(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, "graph1,graph2,graph3");

        // When
        final List<String> graphIds = FederatedStoreUtil.getGraphIds(config);

        // Then
        assertEquals(Arrays.asList("graph1", "graph2", "graph3"), graphIds);
    }

    @Test
    public void shouldGetGraphIdsAndSkipEmptiesAndWhitespace() {
        // Given
        final Map<String, String> config = new HashMap<>();
        config.put(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, " graph1 , graph2,,graph3 ");

        // When
        final List<String> graphIds = FederatedStoreUtil.getGraphIds(config);

        // Then
        assertEquals(Arrays.asList("graph1", "graph2", "graph3"), graphIds);
    }

    @Test
    public void shouldGetEmptyGraphIdsWhenEmptyCsvValue() {
        // Given
        final Map<String, String> config = new HashMap<>();
        config.put(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, "");

        // When
        final List<String> graphIds = FederatedStoreUtil.getGraphIds(config);

        // Then
        assertEquals(Collections.emptyList(), graphIds);
    }

    @Test
    public void shouldGetNullGraphIdsWhenNullCsvValue() {
        // Given
        final Map<String, String> config = new HashMap<>();
        config.put(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, null);

        // When
        final List<String> graphIds = FederatedStoreUtil.getGraphIds(config);

        // Then
        assertNull(graphIds);
    }

    @Test
    public void shouldGetNullGraphIdsWhenNoCsvEntry() {
        // Given
        final Map<String, String> config = new HashMap<>();
        config.put("some other key", "some value");

        // When
        final List<String> graphIds = FederatedStoreUtil.getGraphIds(config);

        // Then
        assertNull(graphIds);
    }

    @Test
    public void shouldGetNullStringsWhenNullCsv() {
        // Given
        final String csv = null;

        // When
        final List<String> values = FederatedStoreUtil.getCleanStrings(csv);

        // Then
        assertNull(values);
    }

    @Test
    public void shouldGetEmptyStringsWhenEmptyCsv() {
        // Given
        final String csv = "";

        // When
        final List<String> values = FederatedStoreUtil.getCleanStrings(csv);

        // Then
        assertEquals(Collections.emptyList(), values);
    }

    @Test
    public void shouldGetCleanStrings() {
        // Given
        final String csv = " 1,2, 3";

        // When
        final List<String> values = FederatedStoreUtil.getCleanStrings(csv);

        // Then
        assertEquals(Arrays.asList("1", "2", "3"), values);
    }

    @Test
    public void shouldGetCleanStringsWithNoEmptiesAndWhitespace() {
        // Given
        final String csv = ", 1 ,2 ,, 3, ";

        // When
        final List<String> values = FederatedStoreUtil.getCleanStrings(csv);

        // Then
        assertEquals(Arrays.asList("1", "2", "3"), values);
    }

    @Test
    public void shouldNotUpdateOperationViewIfNotRequired() {
        // Given
        final Graph graph = createGraph();
        final GetElements operation = new GetElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .build())
                .build();

        // When
        final GetElements updatedOp = FederatedStoreUtil.updateOperationForGraph(operation, graph);

        // Then
        assertSame(operation, updatedOp);
        assertSame(operation.getView(), updatedOp.getView());
    }

    @Test
    public void shouldUpdateOperationView() {
        // Given
        final Graph graph = createGraph();
        final GetElements operation = new GetElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition())
                        .edge(TestGroups.EDGE_2, new ViewElementDefinition())
                        .entity(TestGroups.ENTITY, new ViewElementDefinition())
                        .entity(TestGroups.ENTITY_2, new ViewElementDefinition())
                        .build())
                .build();

        // When
        final GetElements updatedOp = FederatedStoreUtil.updateOperationForGraph(operation, graph);

        // Then
        assertNotSame(operation, updatedOp);
        assertNotSame(operation.getView(), updatedOp.getView());
        assertEquals(Sets.newHashSet(TestGroups.ENTITY), updatedOp.getView().getEntityGroups());
        assertEquals(Sets.newHashSet(TestGroups.EDGE), updatedOp.getView().getEdgeGroups());
        assertSame(operation.getView().getEntity(TestGroups.ENTITY), updatedOp.getView().getEntity(TestGroups.ENTITY));
        assertSame(operation.getView().getEdge(TestGroups.EDGE), updatedOp.getView().getEdge(TestGroups.EDGE));
    }

    @Test
    public void shouldUpdateOperationViewAndReturnNullIfViewHasNoGroups() {
        // Given
        final Graph graph = createGraph();
        final GetElements operation = new GetElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE_2, new ViewElementDefinition())
                        .entity(TestGroups.ENTITY_2, new ViewElementDefinition())
                        .build())
                .build();

        // When
        final GetElements updatedOp = FederatedStoreUtil.updateOperationForGraph(operation, graph);

        // Then
        assertNull(updatedOp);
    }

    @Test
    public void shouldUpdateOperationChainAndReturnNullIfNestedOperationViewHasNoGroups() {
        // Given
        final Graph graph = createGraph();
        final OperationChain<?> operation = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE_2, new ViewElementDefinition())
                                .entity(TestGroups.ENTITY_2, new ViewElementDefinition())
                                .build())
                        .build())
                .build();

        // When
        final OperationChain<?> updatedOp = FederatedStoreUtil.updateOperationForGraph(operation, graph);

        // Then
        assertNull(updatedOp);
    }

    @Test
    public void shouldUpdateNestedOperations() {
        // Given
        final Graph graph = createGraph();
        final HashMap<String, String> options = new HashMap<>();
        options.put("key", "value");
        final HashMap<String, String> options2 = new HashMap<>();
        options2.put("key", "value");
        final GetElements operation = new GetElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE, new ViewElementDefinition())
                        .edge(TestGroups.EDGE_2, new ViewElementDefinition())
                        .entity(TestGroups.ENTITY, new ViewElementDefinition())
                        .entity(TestGroups.ENTITY_2, new ViewElementDefinition())
                        .build())
                .options(options2)
                .build();
        final OperationChain opChain = new OperationChain.Builder()
                .first(operation)
                .options(options)
                .build();

        // When
        final OperationChain<?> updatedOpChain = FederatedStoreUtil.updateOperationForGraph(opChain, graph);

        // Then
        assertNotSame(opChain, updatedOpChain);
        assertEquals(options, updatedOpChain.getOptions());
        assertEquals(1, updatedOpChain.getOperations().size());

        final GetElements updatedOperation = (GetElements) updatedOpChain.getOperations().get(0);
        assertNotSame(operation, updatedOperation);
        assertEquals(options2, updatedOperation.getOptions());
        assertNotSame(operation.getView(), updatedOperation.getView());
        assertEquals(Sets.newHashSet(TestGroups.ENTITY), updatedOperation.getView().getEntityGroups());
        assertEquals(Sets.newHashSet(TestGroups.EDGE), updatedOperation.getView().getEdgeGroups());
        assertSame(operation.getView().getEntity(TestGroups.ENTITY), updatedOperation.getView().getEntity(TestGroups.ENTITY));
        assertSame(operation.getView().getEdge(TestGroups.EDGE), updatedOperation.getView().getEdge(TestGroups.EDGE));
    }

    @Test
    public void shouldNotUpdateAddElementsFlagsWhenNotRequired() {
        // Given
        final Graph graph = createGraph();
        final AddElements operation = new AddElements.Builder()
                .validate(true)
                .skipInvalidElements(true)
                .build();

        // When
        final AddElements updatedOp = FederatedStoreUtil.updateOperationForGraph(operation, graph);

        // Then
        assertSame(operation, updatedOp);
        assertNull(updatedOp.getInput());
    }

    @Test
    public void shouldUpdateAddElementsFlagsWhenNullInputAndValidateFalse() {
        // Given
        final Graph graph = createGraph();
        final AddElements operation = new AddElements.Builder()
                .validate(false)
                .skipInvalidElements(true)
                .build();

        // When
        final AddElements updatedOp = FederatedStoreUtil.updateOperationForGraph(operation, graph);

        // Then
        assertNotSame(operation, updatedOp);
        assertNull(updatedOp.getInput());
        assertTrue(updatedOp.isValidate());
        assertTrue(updatedOp.isSkipInvalidElements());
    }

    @Test
    public void shouldUpdateAddElementsFlagsWhenNullInputAndSkipFalse() {
        // Given
        final Graph graph = createGraph();
        final AddElements operation = new AddElements.Builder()
                .validate(true)
                .skipInvalidElements(false)
                .build();

        // When
        final AddElements updatedOp = FederatedStoreUtil.updateOperationForGraph(operation, graph);

        // Then
        assertNotSame(operation, updatedOp);
        assertNull(updatedOp.getInput());
        assertTrue(updatedOp.isValidate());
        assertTrue(updatedOp.isSkipInvalidElements());
    }

    @Test
    public void shouldUpdateAddElementsInput() {
        // Given
        final Graph graph = createGraph();
        final AddElements operation = new AddElements.Builder()
                .input(new Entity.Builder()
                                .group(TestGroups.ENTITY)
                                .build(),
                        new Entity.Builder()
                                .group(TestGroups.ENTITY_2)
                                .build(),
                        new Edge.Builder()
                                .group(TestGroups.EDGE)
                                .build(),
                        new Edge.Builder()
                                .group(TestGroups.EDGE_2)
                                .build())
                .build();

        // When
        final AddElements updatedOp = FederatedStoreUtil.updateOperationForGraph(operation, graph);

        // Then
        assertNotSame(operation, updatedOp);
        assertNotSame(operation.getInput(), updatedOp.getInput());
        final List<Element> updatedInput = Lists.newArrayList(updatedOp.getInput());
        assertEquals(
                Arrays.asList(new Entity.Builder()
                                .group(TestGroups.ENTITY)
                                .build(),
                        new Edge.Builder()
                                .group(TestGroups.EDGE)
                                .build()),
                updatedInput);
    }

    protected Graph createGraph() {
        final Store store = mock(Store.class);
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .aggregate(false)
                        .build())
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source(TestTypes.ID_STRING)
                        .destination(TestTypes.ID_STRING)
                        .directed(TestTypes.DIRECTED_TRUE)
                        .aggregate(false)
                        .build())
                .type(TestTypes.ID_STRING, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .build())
                .type(TestTypes.DIRECTED_TRUE, new TypeDefinition.Builder()
                        .clazz(Boolean.class)
                        .validateFunctions(new IsTrue())
                        .build())
                .build();

        given(store.getSchema()).willReturn(schema);
        given(store.getOriginalSchema()).willReturn(schema);

        StoreProperties storeProperties = new StoreProperties();

        given(store.getProperties()).willReturn(storeProperties);

        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .build())
                .store(store)
                .build();
    }
}