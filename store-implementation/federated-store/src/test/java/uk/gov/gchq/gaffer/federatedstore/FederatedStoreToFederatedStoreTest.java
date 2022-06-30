/*
 * Copyright 2020-2022 Crown Copyright
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

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.proxystore.ProxyProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_EDGE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_ENTITY;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SCHEMA_EDGE_BASIC_JSON;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.SCHEMA_ENTITY_BASIC_JSON;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.edgeBasic;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.entityBasic;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadSchemaFromJson;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;

/**
 * The FederatedStoreToFederatedStore Test works as follows:
 *                                           --------------------
 *      FederatedStore                      |   GAFFER REST API |
 *           -> Proxy Store --------------> |                   |
 *                                          |   FederatedStore  |
 *                                          |   -> MapStore     |
 *                                          --------------------
 */
public class FederatedStoreToFederatedStoreTest {

    private Graph federatedStoreGraph;
    private Graph restApiFederatedGraph;

    @BeforeEach
    public void setUpStores() throws OperationException {
        resetForFederatedTests();


        ProxyProperties proxyProperties = new ProxyProperties();
        proxyProperties.setStoreClass(SingleUseFederatedStore.class);

        restApiFederatedGraph = new Graph.Builder()
                .storeProperties(proxyProperties)
                .config(new GraphConfig("RestApiGraph"))
                .addSchema(new Schema())
                .build();

        federatedStoreGraph = new Graph.Builder()
                .config(new GraphConfig("federatedStoreGraph"))
                .storeProperties(new FederatedStoreProperties())
                .build();

        connectGraphs();
        addMapStore();
    }


    private void addMapStore() throws OperationException {
        restApiFederatedGraph.execute(
                new AddGraph.Builder()
                        .storeProperties(new MapStoreProperties())
                        .graphId("mapStore")
                        .schema(loadSchemaFromJson(SCHEMA_ENTITY_BASIC_JSON))
                        .build(), new User());
    }

    private void connectGraphs() throws OperationException {
        federatedStoreGraph.execute(
                new AddGraph.Builder()
                        .storeProperties(new ProxyProperties())
                        .graphId("RestProxy")
                        .schema(new Schema())
                        .build(), new User());
    }

    @AfterAll
    public static void afterAll() {
        resetForFederatedTests();
    }

    @Test
    public void shouldErrorIfViewIsInvalid() throws OperationException {
        // Given
        restApiFederatedGraph.execute(
                new AddElements.Builder()
                        .input(entityBasic())
                        .build(), new User());

        // When
        assertThatExceptionOfType(OperationException.class)
                .isThrownBy(() -> federatedStoreGraph.execute(
                        new GetAllElements.Builder()
                                .view(new View.Builder()
                                        .edge(GROUP_BASIC_EDGE)
                                        .build())
                                .build(), new User()))
                .withStackTraceContaining("View is not valid for graphIds:[mapStore]");
    }

    @Test
    public void shouldMaintainView() throws OperationException {
        // Given
        final String mapStoreGraphId = "mapStoreWithFullSchema";
        restApiFederatedGraph.execute(
                new AddGraph.Builder()
                        .storeProperties(new MapStoreProperties())
                        .graphId(mapStoreGraphId)
                        .schema(loadSchemaFromJson(SCHEMA_ENTITY_BASIC_JSON, SCHEMA_EDGE_BASIC_JSON))
                        .build(), new User());

        Entity entity = entityBasic();

        Edge edge = edgeBasic();

        restApiFederatedGraph.execute(
                new FederatedOperation.Builder()
                        .op(new AddElements.Builder().input(entity, edge).build())
                        .graphIds(mapStoreGraphId)
                        .build(), new User());

        // When
        List<? extends Element> results = Lists.newArrayList(federatedStoreGraph.execute(
                new GetAllElements.Builder()
                        .view(new View.Builder()
                                .entity(GROUP_BASIC_ENTITY)
                                .build())
                        .build(), new User()));

        // Then
        assertThat(results)
                .hasSize(1)
                .first().isEqualTo(entity);
    }

    @Test
    public void shouldBeAbleToSendViewedQueries() throws OperationException {
        // Given
        Entity entity = new Entity.Builder()
                .group(GROUP_BASIC_ENTITY)
                .vertex("myVertex")
                .property("property1", 1)
                .build();

        restApiFederatedGraph.execute(new AddElements.Builder()
                .input(entity)
                .build(), new User());

        // When
        Iterable<? extends Element> results =
                federatedStoreGraph.execute(
                        new GetAllElements.Builder()
                                .view(new View.Builder()
                                        .entity(GROUP_BASIC_ENTITY)
                                        .build())
                                .build(), new User());

        // Then
        assertThat(results)
                .hasSize(1)
                .first().isEqualTo(entity);
    }
}
