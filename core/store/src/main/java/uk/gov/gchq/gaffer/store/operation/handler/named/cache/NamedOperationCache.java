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

package uk.gov.gchq.gaffer.store.operation.handler.named.cache;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.named.operation.cache.CacheOperationFailedException;
import uk.gov.gchq.gaffer.user.User;
import java.util.HashSet;
import java.util.Set;

public class NamedOperationCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(NamedOperationCache.class);
    private static final String CACHE_NAME = "NamedOperation";

    /**
     * If the user is just adding to the cache, ie the overwrite flag is set to false, then no security is added.
     * However if the user is overwriting the named operation stored in the cache, then their opAuths must be checked
     * against the write roles associated with the {@link NamedOperationDetail}. If it turns out the user is overwriting a
     * non-existent NamedOperationDetail, then the users NamedOperationDetail will be added normally.
     *
     * @param namedOperation    The NamedOperationDetail that the user wants to store
     * @param overwrite         Flag relating to whether the user is adding (false) or updating/overwriting (true).
     * @param user              The user making the request
     * @throws CacheOperationFailedException thrown if the user doesn't have write access to the NamedOperationDetail requested,
     *                                       or if the add operation fails for some reason.
     */
    public void addNamedOperation(final NamedOperationDetail namedOperation, final boolean overwrite, final User user) throws CacheOperationFailedException {
        String name;
        try {
            name = namedOperation.getOperationName();
        } catch (final NullPointerException e) {
            throw new CacheOperationFailedException("NamedOperation cannot be null", e);
        }
        if (name == null) {
            throw new CacheOperationFailedException("NamedOperation name cannot be null");
        }
        if (!overwrite) {
            addToCache(name, namedOperation, false);
            return;
        }

        NamedOperationDetail existing = null;

        try {
            existing = getFromCache(name);
        } catch (final CacheOperationFailedException e) { // if there is no existing named Operation add one
            addToCache(name, namedOperation, false);
            return;
        }
        if (existing.hasWriteAccess(user)) {
            addToCache(name, namedOperation, true);
        } else {
            throw new CacheOperationFailedException("User " + namedOperation.getCreatorId() + " does not have permission to overwrite");
        }
    }

    /**
     * Checks whether a {@link User} has write access to the cache. If they do then the NamedOperationDetail and name is
     * removed from the cache. If they don't or the NamedOperationDetail doesn't exist then an Exception is thrown.
     *
     * @param name The name of the NamedOperationDetail a user would like to delete
     * @param user A {@link User} object that can optionally be used for checking permissions
     * @throws CacheOperationFailedException Thrown when the NamedOperationDetail doesn't exist or the User doesn't have
     *                                       write permission on the NamedOperationDetail.
     */
    public void deleteNamedOperation(final String name, final User user) throws CacheOperationFailedException {
        if (name == null) {
            throw new CacheOperationFailedException("NamedOperation name cannot be null");
        }
        NamedOperationDetail existing = getFromCache(name);
        if (existing.hasWriteAccess(user)) {
            deleteFromCache(name);
        } else {
            throw new CacheOperationFailedException("User " + user +
                    " does not have authority to delete named operation: " + name);
        }
    }

    /**
     * First gets the NamedOperationDetail in question and checks whether the user has read access before returning the value.
     * If the NamedOperationDetail doesn't exist or the User doesn't have permission to read this NamedOperationDetail, then an
     * exception is thrown.
     *
     * @param name The name of the NamedOperationDetail held in the cache.
     * @param user The {@link User} object that is used for checking read permissions.
     * @return NamedOperationDetail
     * @throws CacheOperationFailedException thrown if the NamedOperationDetail doesn't exist or the User doesn't have permission
     *                                       to read it.
     */
    public NamedOperationDetail getNamedOperation(final String name, final User user) throws CacheOperationFailedException {
        NamedOperationDetail op = getFromCache(name);
        if (op.hasReadAccess(user)) {
            return op;
        } else {
            throw new CacheOperationFailedException("User: " + user + " does not have read access to " + name);
        }
    }

    public CloseableIterable<NamedOperationDetail> getAllNamedOperations(final User user) {
        Set<String> keys = CacheServiceLoader.getService().getAllKeysFromCache(CACHE_NAME);
        Set<NamedOperationDetail> executables = new HashSet<>();
        for (final String key : keys) {
            try {
                NamedOperationDetail op = getFromCache(key);
                if (op.hasReadAccess(user)) {
                    executables.add(op);
                }
            } catch (CacheOperationFailedException e) {
                LOGGER.error(e.getMessage(), e);
            }

        }
        return new WrappedCloseableIterable<>(executables);
    }

    public void clear() throws CacheOperationFailedException {
        try {
            CacheServiceLoader.getService().clearCache(CACHE_NAME);
        } catch (CacheOperationException e) {
            throw new CacheOperationFailedException("Failed to clear cache", e);
        }
    }

    public void deleteFromCache(final String name) throws CacheOperationFailedException {
        CacheServiceLoader.getService().removeFromCache(CACHE_NAME, name);

        if (CacheServiceLoader.getService().getFromCache(CACHE_NAME, name) != null) {
            throw new CacheOperationFailedException("Failed to remove " + name + " from cache");
        }
    }

    public void addToCache(final String name, final NamedOperationDetail operation, final boolean overwrite) throws CacheOperationFailedException {
        try {
            if (overwrite) {
                CacheServiceLoader.getService().putInCache(CACHE_NAME, name, operation);
            } else {
                CacheServiceLoader.getService().putSafeInCache(CACHE_NAME, name, operation);
            }
        } catch (CacheOperationException e) {
            throw new CacheOperationFailedException(e);
        }
    }

    public NamedOperationDetail getFromCache(final String name) throws CacheOperationFailedException {
        if (name == null) {
            throw new CacheOperationFailedException("Operation name cannot be null");
        }
        NamedOperationDetail op = CacheServiceLoader.getService().getFromCache(CACHE_NAME, name);

        if (op != null) {
            return op;
        }
        throw new CacheOperationFailedException("No named operation with the name " + name + " exists in the cache");
    }
}
