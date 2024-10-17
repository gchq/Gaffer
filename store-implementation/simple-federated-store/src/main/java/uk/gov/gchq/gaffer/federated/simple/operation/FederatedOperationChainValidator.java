package uk.gov.gchq.gaffer.federated.simple.operation;

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.operation.OperationChainValidator;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.gaffer.user.User;

public class FederatedOperationChainValidator extends OperationChainValidator {

    public FederatedOperationChainValidator(final ViewValidator viewValidator) {
        super(viewValidator);
    }

    @Override
    protected Schema getSchema(final Operation operation, final User user, final Store store) {
        try {
            return store.execute(new GetSchema.Builder().options(operation.getOptions()).build(), new Context(user));
        } catch (final OperationException e) {
            throw new GafferRuntimeException("Unable to get merged schema for graph " + store.getGraphId(), e);
        }
    }
}
