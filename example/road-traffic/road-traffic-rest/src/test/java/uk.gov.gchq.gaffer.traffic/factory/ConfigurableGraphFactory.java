package uk.gov.gchq.gaffer.traffic.factory;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

public class ConfigurableGraphFactory implements GraphFactory  {
    private Graph graph;

    public static final String STORE_TYPE_PROPERTY = "store.type";
    public static final String STORE_TYPE_DEFAULT = "accumulo";

    private final StoreProperties storeProperties;
    private final Schema schema;
    private final GraphConfig graphConfig;

    public ConfigurableGraphFactory() {
        storeProperties = StoreProperties.loadStoreProperties(StreamUtil.openStream(getClass(),
            System.getProperty(STORE_TYPE_PROPERTY, STORE_TYPE_DEFAULT) + StreamUtil.STORE_PROPERTIES));

        schema = Schema.fromJson(StreamUtil.openStream(getClass(), "/schema/elements.json"),
            StreamUtil.openStream(getClass(), "/schema/types.json"));

        graphConfig = new GraphConfig.Builder()
            .json(StreamUtil.graphConfig(getClass())).build();
    }

    @Override
    public Graph.Builder createGraphBuilder() {
        return new Graph.Builder()
            .addSchema(schema)
            .storeProperties(storeProperties)
            .config(graphConfig);
    }

    @Override
    public Graph getGraph() {
        if (graph == null) {
            graph = createGraph();
        }
        return graph;
    }
}
