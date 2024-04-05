package uk.gov.gchq.gaffer.tinkerpop.util;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

public class GafferPopFederatedTestUtil {
    public static final String VERTEX_PERSON_1 = "person1";
    public static final String VERTEX_PERSON_2 = "person2";
    public static final String VERTEX_SOFTWARE_1 = "software1";
    public static final String VERTEX_SOFTWARE_2 = "software2";
    public static final String SOFTWARE_GROUP = "software";
    public static final String PERSON_GROUP = "person";
    public static final String CREATED_EDGE_GROUP = "created";
    public static final String NAME_PROPERTY = "name";
    public static final String WEIGHT_PROPERTY = "weight";
    private static final User USER = new User("user01");

    private static final FederatedStoreProperties FEDERATED_STORE_PROPERTIES = FederatedStoreProperties.loadStoreProperties("/federatedStore/fed-store.properties");
    private static final MapStoreProperties MAP_STORE_PROPERTIES = MapStoreProperties.loadStoreProperties("/tinkerpop/map-store.properties");
    
    // Creates a basic federated graph with two sub-graphs within it
    public static Graph setUpFederatedGraph(Class<?> clazz) throws Exception {
        final Graph federatedGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("federatedGraph")
                        .build())
                .addStoreProperties(FEDERATED_STORE_PROPERTIES)
                .build();

        federatedGraph.execute(new AddGraph.Builder()
                    .graphId("graphA")
                    .storeProperties(MAP_STORE_PROPERTIES)
                    .schema(Schema.fromJson(StreamUtil.openStreams(clazz, "/gaffer/schema")))
                .build(), USER);

        federatedGraph.execute(new AddGraph.Builder()
                    .graphId("graphB")
                    .storeProperties(MAP_STORE_PROPERTIES)
                    .schema(Schema.fromJson(StreamUtil.openStreams(clazz, "/gaffer/schema")))
                .build(), USER);

        addElements(federatedGraph);

        return federatedGraph;
    }

    // Pre-adds elements to each graph
     public static void addElements(final Graph federatedGraph) throws OperationException {
        federatedGraph.execute(new FederatedOperation.Builder()
            .op(new AddElements.Builder()
                .input(
                    getEntity(PERSON_GROUP, VERTEX_PERSON_1), 
                    getEntity(SOFTWARE_GROUP, VERTEX_SOFTWARE_1),
                    getEdge(CREATED_EDGE_GROUP, VERTEX_PERSON_1, VERTEX_SOFTWARE_1))
                .build())
            .graphIdsCSV("graphA")
            .build(), USER);

        federatedGraph.execute(new FederatedOperation.Builder()
            .op(new AddElements.Builder()
                .input(
                    getEntity(PERSON_GROUP, VERTEX_PERSON_2),
                    getEntity(SOFTWARE_GROUP, VERTEX_SOFTWARE_2),
                    getEdge(CREATED_EDGE_GROUP, VERTEX_PERSON_2, VERTEX_SOFTWARE_2))
                .build())
            .graphIdsCSV("graphB")
            .build(), USER);
    }

    private static Edge getEdge(final String edgeGroup, final String source, final String dest) {
        return new Edge.Builder()
                .group(edgeGroup)
                .source(source)
                .dest(dest)
                .directed(true)
                .build();
    }

    private static Entity getEntity(final String entityGroup, final String vertex) {
        return new Entity.Builder()
                .group(entityGroup)
                .vertex(vertex)
                .build();
    }

}
