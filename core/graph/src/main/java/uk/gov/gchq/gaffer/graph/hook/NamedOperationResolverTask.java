package uk.gov.gchq.gaffer.graph.hook;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedOperationCache;
import uk.gov.gchq.gaffer.user.User;

public class NamedOperationResolverTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(NamedOperationResolverTask.class);

    private Operations<?> operations;
    private User user;
    private NamedOperationCache cache;

    public NamedOperationResolverTask(final Operations<?> operations, final User user, final NamedOperationCache cache) {
        this.operations = operations;
        this.user = user;
        this.cache = cache;
    }

    @Override
    public void run() {
        NamedOperationResolver.resolveNamedOperations(operations, user, cache);
        if (Thread.interrupted()){
            LOGGER.error("resolving named operations is interrupted, exiting");
        }
    }
}
