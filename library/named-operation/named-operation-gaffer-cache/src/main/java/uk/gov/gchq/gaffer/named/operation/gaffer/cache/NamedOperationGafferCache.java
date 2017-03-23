/*
 * Copyright 2016-2017 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.named.operation.gaffer.cache;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
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
        try {
            cache.clear();
        } catch (CacheOperationException e) {
            throw new CacheOperationFailedException("Failed to clear cache", e);
        }
    }

    @Override
    public void deleteFromCache(final String name) throws CacheOperationFailedException {
        cache.remove(name);

        if (cache.get(name) != null) {
            throw new CacheOperationFailedException("Failed to remove " + name + " from cache");
        }
    }

    @Override
    public void addToCache(final String name, final ExtendedNamedOperation operation, final boolean overwrite) throws CacheOperationFailedException {
        try {
            if (overwrite) {
                cache.put(name, operation);
            } else {
                cache.putSafe(name, operation);
            }
        } catch (CacheOperationException e) {
            throw new CacheOperationFailedException(e);
        }
    }

    @Override
    public ExtendedNamedOperation getFromCache(final String name) throws CacheOperationFailedException {
        ExtendedNamedOperation op = cache.get(name);

        if (op != null) {
            return op;
        }
        throw new CacheOperationFailedException("No named operation with the name " + name + " exists in the cache");
    }
}
