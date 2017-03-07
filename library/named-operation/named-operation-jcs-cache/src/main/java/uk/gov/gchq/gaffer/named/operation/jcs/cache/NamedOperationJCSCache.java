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

package uk.gov.gchq.gaffer.named.operation.jcs.cache;

import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.named.operation.ExtendedNamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.cache.AbstractNamedOperationCache;
import uk.gov.gchq.gaffer.named.operation.cache.CacheOperationFailedException;
import uk.gov.gchq.gaffer.user.User;
import java.util.HashSet;
import java.util.Set;

/**
 * Implementation of {@link uk.gov.gchq.gaffer.named.operation.cache.INamedOperationCache} that uses an Apache JCS
 * Cache to store {@link ExtendedNamedOperation}s. This cache is configured using the cache.ccf file.
 */
public final class NamedOperationJCSCache extends AbstractNamedOperationCache {
    public static final String REGION = "namedOperationsRegion";
    private static final String CACHE_GROUP = "NamedOperations";
    private static final Logger LOGGER = LoggerFactory.getLogger(NamedOperationJCSCache.class);
    private JCS cache;
    private String configPath;

    /**
     * Adds a ExtendedNamedOperation to the cache. If the name or ExtendedNamedOperation is null then a {@link CacheOperationFailedException}
     * will be thrown. If the function is called with the overwrite flag set to false then the operation will fail if
     * a ExtendedNamedOperation already exists with the given name.
     *
     * @param name           The name/key to store the ExtendedNamedOperation against
     * @param namedOperation The ExtendedNamedOperation to be stored
     * @param overwrite      Whether or not to overwrite an existing ExtendedNamedOperation
     * @throws CacheOperationFailedException thrown if the operation fails
     */
    public void addToCache(final String name, final ExtendedNamedOperation namedOperation, final boolean overwrite) throws CacheOperationFailedException {
        if (name == null) {
            throw new CacheOperationFailedException("NamedOperation name cannot be null");
        }
        try {
            if (!overwrite && getCache().getFromGroup(name, CACHE_GROUP) != null) {
                throw new CacheOperationFailedException("ExtendedNamedOperation with name " + name + " already exists");
            }
            getCache().putInGroup(name, CACHE_GROUP, namedOperation);

        } catch (CacheException e) {
            LOGGER.error("Failed to add named Operation " + name + " with operation " + namedOperation.getOperationChain().toString(), e.getMessage());
            throw new CacheOperationFailedException(e.getMessage(), e);
        }
    }


    /**
     * Removes a key and value from the cache. If it fails then an exception is thrown
     *
     * @param name The key to delete
     * @throws CacheOperationFailedException Thrown when the delete fails
     */
    public void deleteFromCache(final String name) throws CacheOperationFailedException {
        getCache().remove(name, CACHE_GROUP);
        if (getCache().getFromGroup(name, CACHE_GROUP) != null) {
            throw new CacheOperationFailedException("Failed to remove " + name + " from the cache");
        }
    }


    /**
     * Gets all the keys from the cache and iterates through each ExtendedNamedOperation stored in the cache. Assuming
     * the user has read permission (therefore can execute the NamedOperation) it either adds a NamedOperation or
     * ExtendedNamedOperation to a set, depending on the simple flag which would ordinarily be set to true.
     *
     * @param user   The user making the request
     * @param simple flag determining whether to return a set of NamedOperations with a basic name and description or
     *               the full ExtendedNamedOperation with details of the opChain, and access controls
     * @return a set of NamedOperations
     */
    @Override
    public CloseableIterable<NamedOperation> getAllNamedOperations(final User user, final boolean simple) {
        Set keys = getCache().getGroupKeys(CACHE_GROUP);
        Set<NamedOperation> executables = new HashSet<>();
        for (final Object key : keys) {
            try {
                ExtendedNamedOperation op = getFromCache((String) key);
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


    /**
     * Checks the supplied name is not null, an Object exists with this name in the cache, that Object is not null and
     * is an instance of ExtendedNamedOperation. If all these conditions are met, the ExtendedNamedOperation is returned, if any are not
     * met then an exception is thrown.
     *
     * @param name the key stored in the cache
     * @return a ExtendedNamedOperation stored against the key
     * @throws CacheOperationFailedException thrown when the operation fails
     */
    public ExtendedNamedOperation getFromCache(final String name) throws CacheOperationFailedException {
        if (name == null) {
            throw new CacheOperationFailedException("Operation name cannot be null");
        }
        ExtendedNamedOperation namedOperation;
        Object obj = getCache().getFromGroup(name, CACHE_GROUP);
        if (obj != null && obj.getClass().equals(ExtendedNamedOperation.class)) {
            namedOperation = (ExtendedNamedOperation) obj;
            return namedOperation;
        }
        throw new CacheOperationFailedException("No ExtendedNamedOperation with a name '" + name + "' could be found in the cache");
    }

    /**
     * clears the cache of all keys and values
     *
     * @throws CacheOperationFailedException thrown when the operation fails
     */
    @Override
    public void clear() throws CacheOperationFailedException {
        try {
            getCache().clear();
        } catch (CacheException e) {
            throw new CacheOperationFailedException(e.getMessage(), e);
        }
    }

    public JCS getCache() {
        if (null == cache) {
            if (null != configPath) {
                JCS.setConfigFilename(configPath);
            }
            try {
                cache = JCS.getInstance(REGION);
            } catch (CacheException e1) {
                // Try just the default region
                try {
                    cache = JCS.getInstance("default");
                } catch (CacheException e2) {
                    throw new RuntimeException("Failed to create named operation cache", e2);
                }
            }
        }

        return cache;
    }

    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(final String configPath) {
        this.configPath = configPath;
    }
}
