/*
 * Copyright 2017-2024 Crown Copyright
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

import uk.gov.gchq.gaffer.cache.Cache;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail;
import uk.gov.gchq.gaffer.user.User;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static java.util.Objects.nonNull;

/**
 * Wrapper around the {@link uk.gov.gchq.gaffer.cache.CacheServiceLoader} to provide an interface for handling
 * the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView}s for a Gaffer graph.
 */
public class NamedViewCache extends Cache<String, NamedViewDetail> {
    public static final String CACHE_SERVICE_NAME_PREFIX = "NamedView";
    public static final String NAMED_VIEW_CACHE_SERVICE_NAME = "NamedView";

    public NamedViewCache(final String suffixNamedViewCacheName) {
        super(getCacheNameFrom(suffixNamedViewCacheName), NAMED_VIEW_CACHE_SERVICE_NAME);
    }

    public static String getCacheNameFrom(final String suffixNamedViewCacheName) {
        return Cache.getCacheNameFrom(CACHE_SERVICE_NAME_PREFIX, suffixNamedViewCacheName);
    }

    public String getSuffixCacheName() {
        return getSuffixCacheNameWithoutPrefix(CACHE_SERVICE_NAME_PREFIX);
    }

    /**
     * Adds the supplied {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} to the cache. If the overwrite flag is set to false, and
     * the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} already exists,
     * the Exception thrown will include an overwrite message. Otherwise, the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} with
     * the same name will simply be overwritten.
     * If it turns out the user is overwriting a non-existent {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail}, then the
     * {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} will be added normally.
     *
     * @param namedViewDetail The {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} to store
     * @param overwrite       Flag relating to whether the user is adding (false) or updating/overwriting (true).
     * @throws CacheOperationException if the add operation fails.
     */
    public void addNamedView(final NamedViewDetail namedViewDetail, final boolean overwrite)
            throws CacheOperationException {
        addNamedView(namedViewDetail, overwrite, null, null);
    }

    /**
     * Removes the specified {@link NamedViewDetail} from the cache.
     *
     * @param name {@link NamedViewDetail} name to delete
     * @param user A {@link User} object that can optionally be used for checking permissions
     * @throws CacheOperationException Thrown when the NamedViewDetail doesn't exist or the User doesn't have
     *                                 write permission on the NamedViewDetail
     */
    public void deleteNamedView(final String name, final User user) throws CacheOperationException {
        deleteNamedView(name, user, null);
    }

    /**
     * Gets the specified {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} from the cache.
     *
     * @param name {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} name to get
     * @param user {@link uk.gov.gchq.gaffer.user.User} user requesting view
     * @return namedView {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} of specified name
     * @throws CacheOperationException if the get operation fails
     */
    public NamedViewDetail getNamedView(final String name, final User user) throws CacheOperationException {
        return getNamedView(name, user, null);
    }

    /**
     * Gets the specified {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} from the cache.
     *
     * @param name      {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} name to get
     * @param user      {@link uk.gov.gchq.gaffer.user.User} user requesting view
     * @param adminAuth admin auths
     * @return namedView {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} of specified name
     * @throws CacheOperationException if the get operation fails
     */
    public NamedViewDetail getNamedView(final String name, final User user, final String adminAuth)
            throws CacheOperationException {
        if (nonNull(name)) {
            final NamedViewDetail namedViewDetail = getFromCache(name);
            if (namedViewDetail.hasReadAccess(user, adminAuth)) {
                return namedViewDetail;
            } else {
                throw new CacheOperationException(String.format("User: %s does not have read access to %s", user, name));
            }
        } else {
            throw new CacheOperationException("NamedView name cannot be null");
        }
    }

    /***
     * Gets all the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail}s from the cache for a user.
     *
     * @param user {@link uk.gov.gchq.gaffer.user.User} user requesting views
     * @return a {@link Iterable} containing all of the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail}s in the cache
     * @throws CacheOperationException if the get operation fails
     */
    public Iterable<NamedViewDetail> getAllNamedViews(final User user) throws CacheOperationException {
        return getAllNamedViews(user, null);
    }

    /***
     * Gets all the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail}s from the cache for a user.
     *
     * @param user      {@link uk.gov.gchq.gaffer.user.User} user requesting views
     * @param adminAuth admin auths
     * @return a {@link Iterable} containing all of the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail}s in the cache
     * @throws CacheOperationException if the get operation fails
     */
    public Iterable<NamedViewDetail> getAllNamedViews(final User user, final String adminAuth)
            throws CacheOperationException {
        final Set<NamedViewDetail> views = new HashSet<>();
        for (final String key : super.getAllKeys()) {
            final NamedViewDetail namedViewDetail = getFromCache(key);
            if (namedViewDetail.hasReadAccess(user, adminAuth)) {
                views.add(namedViewDetail);
            }
        }
        return views;
    }

    /**
     * Add the specified {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} to the cache
     *
     * @param namedView the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} to add to the cache
     * @param overwrite if true, overwrite any existing entry which matches the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail}
     *                  name
     * @throws CacheOperationException if the add operation fails
     */
    public void addToCache(final NamedViewDetail namedView, final boolean overwrite) throws CacheOperationException {
        super.addToCache(namedView.getName(), namedView, overwrite);
    }

    /**
     * Get the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} related to the specified name from cache
     *
     * @param name the name of the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} to return
     * @return the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail}
     * @throws CacheOperationException if the get operation fails, or the name does not exist in cache
     */
    @Override
    public NamedViewDetail getFromCache(final String name) throws CacheOperationException {
        if (null != name) {
            final NamedViewDetail namedViewFromCache = super.getFromCache(name);
            if (null != namedViewFromCache) {
                return namedViewFromCache;
            } else {
                throw new CacheOperationException(String.format("No NamedView with the name %s exists in the cache", name));
            }
        } else {
            throw new IllegalArgumentException("NamedView name cannot be null");
        }
    }

    /**
     * Adds the supplied {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} to the cache. If the overwrite flag is set to false, and
     * the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} already exists,
     * the Exception thrown will include an overwrite message. Otherwise, the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} with
     * the same name will simply be overwritten.
     * If it turns out the user is overwriting a non-existent {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail}, then the
     * {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} will be added normally.
     *
     * @param namedViewDetail The {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} to store
     * @param overwrite       Flag relating to whether the user is adding (false) or updating/overwriting (true).
     * @param user            The user making the request.
     * @param adminAuth       The admin auth supplied for permissions.
     * @throws CacheOperationException if the add operation fails.
     */
    public void addNamedView(final NamedViewDetail namedViewDetail, final boolean overwrite, final User user,
                             final String adminAuth)
            throws CacheOperationException {
        String name = namedViewDetail.getName();
        if (name == null) {
            throw new CacheOperationException("NamedView name cannot be null");
        }

        if (overwrite && contains(name)) {
            if (getFromCache(name).hasWriteAccess(user, adminAuth)) {
                addToCache(namedViewDetail, overwrite);
            } else {
                throw new CacheOperationException(String.format("User %s does not have permission to overwrite", user.getUserId()));
            }
        } else {
            addToCache(namedViewDetail, overwrite);
        }
    }

    /**
     * Removes the specified {@link NamedViewDetail} from the cache.
     *
     * @param name      {@link NamedViewDetail} name to delete
     * @param user      A {@link User} object that can optionally be used for checking permissions
     * @param adminAuth The admin auth supplied for permissions.
     * @throws CacheOperationException Thrown when the NamedViewDetail doesn't exist or the User doesn't have
     *                                 write permission on the NamedViewDetail
     */
    public void deleteNamedView(final String name, final User user, final String adminAuth)
            throws CacheOperationException {
        if (Objects.isNull(name)) {
            throw new IllegalArgumentException("NamedView name cannot be null");
        }

        NamedViewDetail existing;
        try {
            existing = getFromCache(name);
        } catch (final CacheOperationException e) {
            // Unable to find the requested entry to delete
            return;
        }

        if (user == null || (existing != null && existing.hasWriteAccess(user, adminAuth))) {
            deleteFromCache(name);
        } else {
            throw new CacheOperationException(String.format("User %s does not have permission to delete named view: %s", user, name));
        }
    }
}
