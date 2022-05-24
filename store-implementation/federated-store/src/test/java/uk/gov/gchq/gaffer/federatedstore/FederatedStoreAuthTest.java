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
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.federatedstore.operation.IFederationOperation;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedAddGraphHandler;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.CACHE_SERVICE_CLASS_STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.EDGES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ENTITIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_ACCUMULO;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;
import static uk.gov.gchq.gaffer.user.StoreUser.AUTH_1;
import static uk.gov.gchq.gaffer.user.StoreUser.authUser;
import static uk.gov.gchq.gaffer.user.StoreUser.blankUser;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedStoreAuthTest {
    private static final AccumuloProperties ACCUMULO_PROPERTIES = loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);
    private final FederatedAddGraphHandler federatedAddGraphHandler = new FederatedAddGraphHandler();
    private FederatedStore federatedStore;
    private IFederationOperation ignore;
    private GetAllGraphIds mock;

    @BeforeEach
    public void setUp() throws Exception {
        resetForFederatedTests();

        federatedStore = new FederatedStore();

        FederatedStoreProperties federatedStoreProperties;
        federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);

        federatedStore.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, federatedStoreProperties);

        mock = Mockito.mock(GetAllGraphIds.class);
    }


    @Test
    public void shouldAddGraphWithAuth() throws Exception {
        //given
        addGraphWith(AUTH_1, new Schema(), testUser());

        //when
        Collection<Graph> testUserGraphs = federatedStore.getGraphs(testUser(), null, mock);
        Collection<Graph> authUserGraphs = federatedStore.getGraphs(authUser(), null, mock);
        Collection<Graph> blankUserGraphs = federatedStore.getGraphs(blankUser(), null, ignore);

        //then
        assertThat(authUserGraphs).hasSize(1);
        assertThat(testUserGraphs).hasSize(1);

        assertThat(authUserGraphs.iterator().next().getGraphId())
                .isEqualTo(GRAPH_ID_ACCUMULO);

        assertThat(testUserGraphs.iterator().next())
                .isEqualTo((authUserGraphs.iterator().next()));

        assertThat(blankUserGraphs).isNotNull().isEmpty();
    }

    @Test
    public void shouldNotShowHiddenGraphsInError() throws Exception {
        //given
        final String unusualType = "unusualType";
        final String groupEnt = ENTITIES + "Unusual";
        final String groupEdge = EDGES + "Unusual";

        Schema schema = new Schema.Builder()
                .entity(groupEnt, new SchemaEntityDefinition.Builder()
                        .vertex(unusualType)
                        .build())
                .edge(groupEdge, new SchemaEdgeDefinition.Builder()
                        .source(unusualType)
                        .destination(unusualType)
                        .directed(DIRECTED_EITHER)
                        .build())
                .type(unusualType, String.class)
                .type(DIRECTED_EITHER, Boolean.class)
                .build();

        addGraphWith(AUTH_1, schema, blankUser());

        final OperationException e = assertThrows(OperationException.class, () -> addGraphWith("nonMatchingAuth", schema, testUser()));

        assertThat(e).message()
                .contains(String.format("Error adding graph %s to storage due to: User is attempting to overwrite a graph within FederatedStore. GraphId: %s", GRAPH_ID_ACCUMULO, GRAPH_ID_ACCUMULO))
                .withFailMessage("error message should not contain details about schema")
                .doesNotContain(unusualType)
                .doesNotContain(groupEdge)
                .doesNotContain(groupEnt);

        assertTrue(federatedStore.getGraphs(testUser(), null, mock).isEmpty());
    }

    private void addGraphWith(final String auth, final Schema schema, final User user) throws OperationException {
        federatedAddGraphHandler.doOperation(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_ACCUMULO)
                        .schema(schema)
                        .storeProperties(ACCUMULO_PROPERTIES.clone())
                        .graphAuths(auth)
                        .build(),
                new Context(user),
                federatedStore);
    }
}
