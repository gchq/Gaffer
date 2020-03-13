package uk.gov.gchq.gaffer.federatedstore.integration;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.PublicAccessPredefinedFederatedStore;
import uk.gov.gchq.gaffer.federatedstore.SingleUseFederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperationChain;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.proxystore.ProxyProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FederatedStoreRecursionIT extends AbstractStoreIT {

    public static final String INNER_FEDERATED_GRAPH = "InnerFederatedGraph";
    public static final String INNER_PROXY = "innerProxy";
    public static final String ENTITY_GRAPH = "entityGraph";
    public static final String OUTER_ENTITY_GRAPH = "Outer" + ENTITY_GRAPH;
    public static final String PROXY_TO_REST_SERVICE_FEDERATED_GRAPH = "proxyToRestServiceFederatedGraph";
    private Graph proxyToRestServiceFederatedGraph;

    @Before
    public void setUp() throws Exception {
        graph.execute(new RemoveGraph.Builder()
                .graphId(PublicAccessPredefinedFederatedStore.ACCUMULO_GRAPH_WITH_EDGES)
                .build(), user);
        graph.execute(new RemoveGraph.Builder()
                .graphId(PublicAccessPredefinedFederatedStore.ACCUMULO_GRAPH_WITH_ENTITIES)
                .build(), user);

        graph = null;
    }


    @Test(timeout = 60000)
    public void shouldNotInfinityLoopWhenAddingElements() throws Exception {
        /*
         * Structure:
         *
         * proxyToRestServiceFederatedGraph --> graphId (named by factory) <------------
         *                                      |                   |                   ^
         *                                      v                   v                   |
         *                                      outerEntityGraph    InnerFederatedStore |
         *                                                          |                   |
         *                                                          v                   |
         *                                                          innerProxy -------->
         */

        createProxyToRestServiceFederatedGraph();
        createdTheInnerFederatedStore();
        createInnerProxyToOuterFederatedStore();
        testOuterGetGraphIds(INNER_FEDERATED_GRAPH);
        testInnerGetGraphIds(INNER_PROXY);
        createOuterEntityGraph();
        testOuterGetGraphIds(INNER_FEDERATED_GRAPH, OUTER_ENTITY_GRAPH);
        addEntity();
        testGetAllElements();
    }

    protected void addEntity() throws OperationException {
        proxyToRestServiceFederatedGraph.execute(
                new AddElements.Builder()
                        .input(new Entity.Builder().vertex("e1").build())
                        .build()
                , user);
    }

    protected void testGetAllElements() throws OperationException {
        ArrayList<Element> elements = Lists.newArrayList(proxyToRestServiceFederatedGraph.execute(new GetAllElements(), user));
        assertEquals(elements.toString(), 1, elements.size());
    }

    protected void createOuterEntityGraph() throws OperationException {
        proxyToRestServiceFederatedGraph.execute(new AddGraph.Builder()
                .graphId(OUTER_ENTITY_GRAPH)
                .storeProperties(new MapStoreProperties())
                .schema(new Schema.Builder()
                        .entity("ent1",
                                new SchemaEntityDefinition.Builder()
                                        .vertex("string")
                                        .build())
                        .type("string", String.class)
                        .build())
                .build(), user);
    }

    protected void testInnerGetGraphIds(final String... ids) throws OperationException {
        ArrayList<? extends String> list = Lists.newArrayList(proxyToRestServiceFederatedGraph.execute(
                new FederatedOperationChain.Builder<Object, String>()
                        .operationChain(OperationChain.wrap(
                                new GetAllGraphIds.Builder()
                                        .option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, INNER_FEDERATED_GRAPH)
                                        .build()
                        )).build()
                , user));
        Assert.assertEquals(ids.length, list.size());
        for (String id : ids) {
            assertTrue(list.toString(), list.contains(id));
        }
    }

    protected void testOuterGetGraphIds(final String... ids) throws OperationException {
        ArrayList<? extends String> list = Lists.newArrayList(proxyToRestServiceFederatedGraph.execute(new GetAllGraphIds(), user));
        Assert.assertEquals(ids.length, list.size());
        for (String id : ids) {
            assertTrue(list.toString(), list.contains(id));
        }
    }

    protected void createInnerProxyToOuterFederatedStore() throws OperationException {
        ProxyProperties storeProperties = new ProxyProperties();
        storeProperties.setReadTimeout(120000);
        storeProperties.setConnectTimeout(120000);
        proxyToRestServiceFederatedGraph.execute(new FederatedOperationChain.Builder<>()
                .operationChain(OperationChain.wrap(new AddGraph.Builder()
                        .graphId(INNER_PROXY)
                        .schema(new Schema())
                        .storeProperties(storeProperties)
                        .build()))
                .build(), user);
    }

    protected void createdTheInnerFederatedStore() throws
            OperationException {
        proxyToRestServiceFederatedGraph.execute(new AddGraph.Builder()
                .graphId(INNER_FEDERATED_GRAPH)
                .schema(new Schema())
                .storeProperties(new FederatedStoreProperties())
                .build(), user);
    }

    protected void createProxyToRestServiceFederatedGraph() {
        final Graph proxyToRestServiceFederatedGraph;
        ProxyProperties singleUseFedProperties = new ProxyProperties();
        singleUseFedProperties.setStoreClass(SingleUseFederatedStore.class);
        singleUseFedProperties.setReadTimeout(120000);
        singleUseFedProperties.setConnectTimeout(120000);

        proxyToRestServiceFederatedGraph = new Graph.Builder()
                .storeProperties(singleUseFedProperties)
                .addSchema(new Schema())
                .config(new GraphConfig(PROXY_TO_REST_SERVICE_FEDERATED_GRAPH))
                .build();
        this.proxyToRestServiceFederatedGraph = proxyToRestServiceFederatedGraph;
    }


}
