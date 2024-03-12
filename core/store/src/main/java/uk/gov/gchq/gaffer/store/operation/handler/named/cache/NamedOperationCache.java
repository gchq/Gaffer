/*
 * Copyright 2016-2024 Crown Copyright
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

import com.google.common.base.Strings;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.cache.Cache;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.user.User;

import java.util.Objects;
import java.util.stream.StreamSupport;

/**
 * Wrapper around the {@link uk.gov.gchq.gaffer.cache.CacheServiceLoader} to provide an interface for handling
 * the {@link uk.gov.gchq.gaffer.named.operation.NamedOperation}s for a Gaffer graph.
 */
public class NamedOperationCache extends Cache<String, NamedOperationDetail> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NamedOperationCache.class);
    public static final String CACHE_SERVICE_NAME_PREFIX = "NamedOperation";
    public static final String NAMED_OPERATION_CACHE_WAS_MADE_WITH_NULL_OR_EMPTY_SUFFIX = "NamedOperation Cache was made with Null or Empty suffix, This is very likely a mistake. GraphId or a supplied suffix is normal";
    public static final String NAMED_OPERATION_CACHE_SERVICE_NAME = "NamedOperation";

    public NamedOperationCache(final String suffixNamedOperationCacheName) {
        super(getCacheNameFrom(suffixNamedOperationCacheName), NAMED_OPERATION_CACHE_SERVICE_NAME);
        if (Strings.isNullOrEmpty(suffixNamedOperationCacheName)) {
            LOGGER.error(NAMED_OPERATION_CACHE_WAS_MADE_WITH_NULL_OR_EMPTY_SUFFIX);
        }
    }

    public static String getCacheNameFrom(final String suffixNamedOperationCacheName) {
        return Cache.getCacheNameFrom(CACHE_SERVICE_NAME_PREFIX, suffixNamedOperationCacheName);
    }

    public String getSuffixCacheName() {
        return getSuffixCacheNameWithoutPrefix(CACHE_SERVICE_NAME_PREFIX);
    }

    /**
     * If the user is just adding to the cache, ie the overwrite flag is set to false, then no security is added.
     * However if the user is overwriting the named operation stored in the cache, then their opAuths must be checked
     * against the write roles associated with the {@link NamedOperationDetail}. If it turns out the user is overwriting a
     * non-existent NamedOperationDetail, then the users NamedOperationDetail will be added normally.
     *
     * @param namedOperation The NamedOperationDetail that the user wants to store.
     * @param overwrite      Flag relating to whether the user is adding (false) or updating/overwriting (true).
     * @param user           The user making the request.
     * @throws CacheOperationException thrown if the user doesn't have write access to the NamedOperationDetail requested,
     *                                 or if the add operation fails for some reason.
     */
    public void addNamedOperation(final NamedOperationDetail namedOperation, final boolean overwrite, final User user) throws CacheOperationException {
        addNamedOperation(namedOperation, overwrite, user, null);
    }

    /**
     * Checks whether a {@link User} has write access to the cache. If they do then the NamedOperationDetail and name is
     * removed from the cache. If they don't or the NamedOperationDetail doesn't exist then an Exception is thrown.
     *
     * @param name The name of the NamedOperationDetail a user would like to delete.
     * @param user A {@link User} object that can optionally be used for checking permissions.
     * @throws CacheOperationException Thrown when the NamedOperationDetail doesn't exist or the User doesn't have
     *                                 write permission on the NamedOperationDetail.
     */
    public void deleteNamedOperation(final String name, final User user) throws CacheOperationException {
        deleteNamedOperation(name, user, null);
    }

    /**
     * First gets the NamedOperationDetail in question and checks whether the user has read access before returning the value.
     * If the NamedOperationDetail doesn't exist or the User doesn't have permission to read this NamedOperationDetail, then an
     * exception is thrown.
     *
     * @param name The name of the NamedOperationDetail held in the cache.
     * @param user The {@link User} object that is used for checking read permissions.
     * @return NamedOperationDetail.
     * @throws CacheOperationException thrown if the NamedOperationDetail doesn't exist or the User doesn't have permission
     *                                 to read it.
     */
    public NamedOperationDetail getNamedOperation(final String name, final User user) throws CacheOperationException {
        return getNamedOperation(name, user, null);
    }

    /**
     * Get all the named operations held in the cache.
     *
     * @param user The {@link User} object that is used for checking read permissions.
     * @return a {@link Iterable} containing the named operation details
     */
    public Iterable<NamedOperationDetail> getAllNamedOperations(final User user) {
        return getAllNamedOperations(user, null);
    }


    /**
     * Retrieve the specified named operation from the cache.
     *
     * @param name the name of the named operation to retrieve
     * @return the details of the requested named operation
     * @throws CacheOperationException if there was an error accessing the
     *                                 cache
     */
    @Override
    public NamedOperationDetail getFromCache(final String name) throws CacheOperationException {
        if (Objects.isNull(name)) {
            throw new CacheOperationException("Operation name cannot be null");
        }
        final NamedOperationDetail op = super.getFromCache(name);

        if (null == op) {
            throw new CacheOperationException("No named operation with the name " + name + " exists in the cache:" + cacheName);
        } else {
            return op;
        }
    }

    /**
     * If the user is just adding to the cache, ie the overwrite flag is set to false, then no security is added.
     * However if the user is overwriting the named operation stored in the cache, then their opAuths must be checked
     * against the write roles associated with the {@link NamedOperationDetail}. If it turns out the user is overwriting a
     * non-existent NamedOperationDetail, then the users NamedOperationDetail will be added normally.
     *
     * @param namedOperation The NamedOperationDetail that the user wants to store.
     * @param overwrite      Flag relating to whether the user is adding (false) or updating/overwriting (true).
     * @param user           The user making the request.
     * @param adminAuth      The admin auth supplied for permissions.
     * @throws CacheOperationException thrown if the user doesn't have write access to the NamedOperationDetail requested,
     *                                 or if the add operation fails for some reason.
     */
    @SuppressFBWarnings(value = "DCN_NULLPOINTER_EXCEPTION", justification = "Investigate an improved null checking approach")
    public void addNamedOperation(final NamedOperationDetail namedOperation, final boolean overwrite, final User user, final String adminAuth) throws CacheOperationException {
        if (namedOperation == null) {
            throw new CacheOperationException("NamedOperation cannot be null");
        }
        try {
            String name = namedOperation.getOperationName();
            if (overwrite && contains(name)) {
                final boolean doesUserHavePermissionToWrite = getFromCache(name).hasWriteAccess(user, adminAuth);
                if (doesUserHavePermissionToWrite) {
                    addToCache(name, namedOperation, true);
                } else {
                    throw new CacheOperationException(String.format("User %s does not have permission to overwrite", user.getUserId()));
                }
            } else {
                addToCache(name, namedOperation, overwrite);
            }
        } catch (final GafferRuntimeException e) {
            throw new CacheOperationException(e);
        }
    }

    /**
     * Checks whether a {@link User} has write access to the cache. If they do then the NamedOperationDetail and name is
     * removed from the cache. If they don't or the NamedOperationDetail doesn't exist then an Exception is thrown.
     *
     * @param name      The name of the NamedOperationDetail a user would like to delete.
     * @param user      A {@link User} object that can optionally be used for checking permissions.
     * @param adminAuth The admin auth supplied for permissions.
     * @throws CacheOperationException Thrown when the NamedOperationDetail doesn't exist or the User doesn't have
     *                                 write permission on the NamedOperationDetail.
     */
    public void deleteNamedOperation(final String name, final User user, final String adminAuth) throws CacheOperationException {
        if (Objects.isNull(name)) {
            throw new CacheOperationException("NamedOperation name cannot be null");
        }

        NamedOperationDetail existing;
        try {
            existing = getFromCache(name);
        } catch (final CacheOperationException e) {
            // Unable to find the requested entry to delete
            return;
        }

        if (existing.hasWriteAccess(user, adminAuth)) {
            super.deleteFromCache(name);
        } else {
            throw new CacheOperationException(String.format("User %s does not have authority to delete named operation: %s", user, name));
        }
    }


    /**
     * First gets the NamedOperationDetail in question and checks whether the user has read access before returning the value.
     * If the NamedOperationDetail doesn't exist or the User doesn't have permission to read this NamedOperationDetail, then an
     * exception is thrown.
     *
     * @param name      The name of the NamedOperationDetail held in the cache.
     * @param user      The {@link User} object that is used for checking read permissions.
     * @param adminAuth The admin auth supplied for permissions.
     * @return NamedOperationDetail.
     * @throws CacheOperationException thrown if the NamedOperationDetail doesn't exist or the User doesn't have permission
     *                                 to read it.
     */
    public NamedOperationDetail getNamedOperation(final String name, final User user, final String adminAuth) throws CacheOperationException {
        final NamedOperationDetail op = getFromCache(name);
        if (op.hasReadAccess(user, adminAuth)) {
            return op;
        } else {
            throw new CacheOperationException(String.format("User: %s does not have read access to %s", user, name));
        }
    }

    /**
     * Get all the named operations held in the cache.
     *
     * @param user      The {@link User} object that is used for checking read permissions.
     * @param adminAuth The admin auth supplied for permissions.
     * @return a {@link Iterable} containing the named operation details
     */
    public Iterable<NamedOperationDetail> getAllNamedOperations(final User user, final String adminAuth) {
        return () -> StreamSupport.stream(getAllKeys().spliterator(), false)
            .map(key -> {
                try {
                    return getFromCache(key);
                } catch (final CacheOperationException e) {
                    LOGGER.error(e.getMessage(), e);
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .filter(op -> op.hasReadAccess(user, adminAuth))
            .iterator();
    }
}
