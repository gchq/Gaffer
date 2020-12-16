package uk.gov.gchq.gaffer.integration.factory;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.integration.util.TestUtil;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

/**
 * The Graph factory used by the remote REST API which backs the ProxyStore. It provides an easy mechanism for resetting
 * the graph which is useful for resetting state between tests without having to restart the whole REST API.
 */
public class MapStoreGraphFactory implements GraphFactory {
    private static final StoreProperties STORE_PROPERTIES = StoreProperties.loadStoreProperties(StreamUtil.openStream(MapStoreGraphFactory.class, "/stores/mapstore.properties"));
    private Graph instance;
    private Schema schema;

    public MapStoreGraphFactory() {
        schema = TestUtil.createDefaultSchema();
        instance = createGraphBuilder().build();
    }


    @Override
    public Graph.Builder createGraphBuilder() {
        return new Graph.Builder()
                .storeProperties(STORE_PROPERTIES)
                .config(new GraphConfig("proxyStoreTest"))
                .addSchema(schema);
    }

    @Override
    public Graph getGraph() {
        return instance;
    }

    public void reset() {
        reset(TestUtil.createDefaultSchema());
    }

    public void reset(final Schema schema) {
        this.schema = schema;
        instance = createGraphBuilder().build();
    }
}
