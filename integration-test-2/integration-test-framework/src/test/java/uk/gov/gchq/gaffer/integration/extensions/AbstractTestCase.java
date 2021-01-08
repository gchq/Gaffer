package uk.gov.gchq.gaffer.integration.extensions;

import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.store.StoreProperties;

public abstract class AbstractTestCase {
    private final StoreProperties storeProperties;

    public AbstractTestCase(final StoreProperties storeProperties) {
        this.storeProperties = storeProperties;
    }

    public StoreProperties getStoreProperties() {
        return storeProperties;
    }

    protected abstract Graph getGraph();

    protected String getTestName() {
        String storeClass = storeProperties.getStoreClass();
        return storeClass.substring(storeClass.lastIndexOf(".") + 1);
    }
}
