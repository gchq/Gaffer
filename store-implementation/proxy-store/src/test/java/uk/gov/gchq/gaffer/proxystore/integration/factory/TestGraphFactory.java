package uk.gov.gchq.gaffer.proxystore.integration.factory;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

public class TestGraphFactory implements GraphFactory {
    private static final StoreProperties MAP_STORE_PROPERTIES = MapStoreProperties.loadStoreProperties(StreamUtil.openStream(TestGraphFactory.class, "/map-store.properties"));
    private static final Schema SCHEMA = Schema.fromJson(StreamUtil.schema(TestGraphFactory.class));
    @Override
    public Graph.Builder createGraphBuilder() {
        return new Graph.Builder()
                .storeProperties(MAP_STORE_PROPERTIES)
                .addSchema(SCHEMA)
                .config(new GraphConfig("myGraph"));
    }

    @Override
    public Graph getGraph() {
        return createGraphBuilder().build();
    }

}
