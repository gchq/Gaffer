package uk.gov.gchq.gaffer.named.operation.gaffer.cache;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.named.operation.ExtendedNamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.cache.AbstractNamedOperationCache;
import uk.gov.gchq.gaffer.named.operation.cache.CacheOperationFailedException;
import uk.gov.gchq.gaffer.user.User;

import java.util.HashSet;
import java.util.Set;

public class NamedOperationGafferCache extends AbstractNamedOperationCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(NamedOperationGafferCache.class);
    private static final String CACHE_NAME = "NamedOperation";
    private ICache<String, ExtendedNamedOperation> cache = CacheServiceLoader.getService().getCache(CACHE_NAME);


    @Override
    public CloseableIterable<NamedOperation> getAllNamedOperations(final User user, final boolean simple) {
        Set<String> keys = cache.getAllKeys();
        Set<NamedOperation> executables = new HashSet<>();
        for (final String key : keys) {
            try {
                ExtendedNamedOperation op = getFromCache(key);
                if (op.hasReadAccess(user)) {
                    if (simple) {
                        executables.add(op.getBasic());
                    } else {
                        executables.add(op);
                    }
                }
            } catch (CacheOperationFailedException e) {
                LOGGER.error(e.getMessage(), e);
            }

        }
        return new WrappedCloseableIterable<>(executables);
    }

    @Override
    public void clear() throws CacheOperationFailedException {
        cache.clear();
    }

    @Override
    public void deleteFromCache(final String name) throws CacheOperationFailedException {
        cache.remove(name);
    }

    @Override
    public void addToCache(final String name, final ExtendedNamedOperation operation, boolean overwrite) throws CacheOperationFailedException {
        cache.put(name, operation);
    }

    @Override
    public ExtendedNamedOperation getFromCache(String name) throws CacheOperationFailedException {
        return cache.get(name);
    }
}
