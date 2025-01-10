package uk.gov.gchq.gaffer.federated.simple.util;

import java.util.ArrayList;
import java.util.List;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federated.simple.operation.AddGraph;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.FederatedOperationHandler;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties.PROP_DEFAULT_GRAPH_IDS;
import static uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties.PROP_DEFAULT_MERGE_ELEMENTS;

public class FederatedModernTestUtil {
    public static final String FEDERATED_GRAPH_ID = "simpleFederatedGraph";
    public static final String CREATED_GRAPH_ID = "createdGraph";
    public static final String KNOWS_GRAPH_ID = "knowsGraph";
    private static final Context CONTEXT = new Context();

    public static Graph setUpSimpleFederatedGraph(Class<?> clazz, StoreProperties properties) throws OperationException {
        FederatedStoreProperties fedProperties = new FederatedStoreProperties();
        fedProperties.set(PROP_DEFAULT_GRAPH_IDS, CREATED_GRAPH_ID + "," + KNOWS_GRAPH_ID);
        fedProperties.set(PROP_DEFAULT_MERGE_ELEMENTS, "true");
        final Graph federatedGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(FEDERATED_GRAPH_ID)
                        .build())
                .addStoreProperties(fedProperties)
                .build();

        federatedGraph.execute(
            new AddGraph.Builder()
                .graphConfig(new GraphConfig(KNOWS_GRAPH_ID))
                .schema(Schema.fromJson(StreamUtil.openStreams(clazz, "/modern/schema")))
                .properties(properties.getProperties())
                .build(), CONTEXT);

        federatedGraph.execute(
            new AddGraph.Builder()
                .graphConfig(new GraphConfig(CREATED_GRAPH_ID))
                .schema(Schema.fromJson(StreamUtil.openStreams(clazz, "/modern/schema")))
                .properties(properties.getProperties())
                .build(), CONTEXT);

        setupKnowsGraph(federatedGraph);
        setupCreatedGraph(federatedGraph);

        return federatedGraph;
    }

    private static void setupKnowsGraph(final Graph federatedGraph) throws OperationException {
        List<Element> knowsGraphElements = new ArrayList<>();

        knowsGraphElements.add(getEntity("1", "person", "marko"));
        knowsGraphElements.add(getEntity("4", "person", "josh"));
        knowsGraphElements.add(getEntity("2", "person", "vadas"));
        knowsGraphElements.add(getEntity("6", "person", "peter"));
        knowsGraphElements.add(getEdge("1", "4", "knows"));
        knowsGraphElements.add(getEdge("1", "2", "knows"));

        federatedGraph.execute(new AddElements.Builder()
                        .input(knowsGraphElements.toArray(new Element[0]))
                        .option(FederatedOperationHandler.OPT_GRAPH_IDS, KNOWS_GRAPH_ID)
                        .build(), CONTEXT);
    }

    private static void setupCreatedGraph(final Graph federatedGraph) throws OperationException {
        List<Element> createdGraphElements = new ArrayList<>();

        createdGraphElements.add(getEntity("3", "software", "lop"));
        createdGraphElements.add(getEntity("5", "software", "ripple"));
        createdGraphElements.add(getEntity("1", "person", "marko"));
        createdGraphElements.add(getEntity("4", "person", "josh"));
        createdGraphElements.add(getEntity("6", "person", "peter"));
        createdGraphElements.add(getEdge("1", "3", "created"));
        createdGraphElements.add(getEdge("4", "3", "created"));
        createdGraphElements.add(getEdge("6", "3", "created"));
        createdGraphElements.add(getEdge("4", "5", "created"));

        federatedGraph.execute(new AddElements.Builder()
                        .input(createdGraphElements.toArray(new Element[0]))
                        .option(FederatedOperationHandler.OPT_GRAPH_IDS, CREATED_GRAPH_ID)
                        .build(), CONTEXT);
    }

    private static Entity getEntity(final String vertex, final String group, final String name) {
        return new Entity.Builder()
            .group(group)
            .vertex(vertex)
            .property("name", name)
            .build();
    }

    private static Edge getEdge(final String source, final String dest, final String group) {
        return new Edge.Builder()
                .group(group)
                .source(source)
                .dest(dest)
                .directed(true)
                .build();
    }
}
