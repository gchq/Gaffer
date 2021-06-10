package uk.gov.gchq.gaffer.proxystore.operation.handler;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.proxystore.ProxyProperties;
import uk.gov.gchq.gaffer.proxystore.operation.GetUrlOperation;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

public class GetUrlHandler implements OutputOperationHandler<GetUrlOperation, String> {
    @Override
    public String doOperation(final GetUrlOperation operation, final Context context, final Store store) throws OperationException {
        try {
            return new ProxyProperties(store.getProperties().getProperties()).getGafferUrl().toString();
        } catch (Exception e) {
            throw new OperationException("Error getting url", e);
        }
    }
}
