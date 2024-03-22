package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.join.Join;
import uk.gov.gchq.gaffer.store.operation.handler.join.JoinHandler;

public class FederatedStoreJoinHandler<I> extends JoinHandler<I> {

    @Override
    protected Operation getOperationFromJoin(final Join<I> join) {
        //TODO it is likely that other Option changes should be preserved and restored.
        return super.getOperationFromJoin(join).shallowClone();
    }
}
