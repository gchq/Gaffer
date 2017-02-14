package uk.gov.gchq.gaffer.named.operation.cache;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.named.operation.ExtendedNamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.user.User;
import java.util.HashMap;
import java.util.HashSet;

public class MockNamedOperationCache implements INamedOperationCache {

    private HashMap<String, ExtendedNamedOperation> fakeCache = new HashMap<>();

    @Override
    public void addNamedOperation(ExtendedNamedOperation operation, boolean overWrite, User user) throws CacheOperationFailedException {
        fakeCache.put(operation.getOperationName(), operation);
    }

    @Override
    public void deleteNamedOperation(String name, User user) throws CacheOperationFailedException {
        fakeCache.remove(name);
    }

    @Override
    public ExtendedNamedOperation getNamedOperation(String name, User user) throws CacheOperationFailedException {
        return fakeCache.get(name);
    }

    @Override
    public CloseableIterable<NamedOperation> getAllNamedOperations(User user, boolean simple) {
        return new WrappedCloseableIterable<>(new HashSet<>(fakeCache.values()));
    }

    @Override
    public void clear() throws CacheOperationFailedException {
        fakeCache.clear();
    }
}
