package uk.gov.gchq.gaffer.integration.extensions;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.integration.util.TestUtil;
import uk.gov.gchq.gaffer.store.StoreProperties;

/**
 * A POJO which is injected into Gaffer tests. It provides the test a way to create graphs easily. To create a graph,
 * you only need to provide some store properties. The graphs will then be created lazily.
 */
public class GafferTestCase {

    private StoreProperties storeProperties;
    private Graph graph;

    public GafferTestCase(final StoreProperties storeProperties) {
        this.storeProperties = storeProperties;
    }

    public String getStoreType() {
        return this.storeProperties.getStoreClass();
    }

    public StoreProperties getStoreProperties() {
        return storeProperties;
    }

    public Graph createEmptyGraph() {
        return new Graph.Builder()
                .config(new GraphConfig("test"))
                .addSchema(TestUtil.createDefaultSchema())
                .storeProperties(this.storeProperties)
                .build();
    }

    public Graph getEmptyGraph() {
        if (graph == null) {
            graph = createEmptyGraph();
        }

        return graph;
    }

    public Graph getPopulatedGraph() {
        return TestUtil.addDefaultElements(getEmptyGraph());
    }
}
