package uk.gov.gchq.gaffer.federatedstore;

public class SingleUseFederatedStore extends uk.gov.gchq.gaffer.proxystore.SingleUseProxyStore {

    protected String getPathToDelegateProperties() {
        return "properties/singleUseFederatedStore.properties";
    }
}
