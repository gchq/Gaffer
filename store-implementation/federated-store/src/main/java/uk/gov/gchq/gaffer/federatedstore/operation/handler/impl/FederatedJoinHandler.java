package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.join.Join;
import uk.gov.gchq.gaffer.store.operation.handler.join.JoinHandler;

public class FederatedJoinHandler<I> extends JoinHandler<I> {


    /**
     * Gets an Operation from a Join, but with a clone.
     * This avoids a false looping error being detected by the FederatedStore.
     *
     * @param join The Join to get operation from
     * @return The Operation with a deep clone of the options field.
     */
    @Override
    protected Operation getOperationFromJoin(final Join<I> join) {
        return super.getOperationFromJoin(join).shallowClone();
    }
}
