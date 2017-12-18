/*
 * Copyright 2017 Crown Copyright
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

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail;
import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;

import java.util.HashSet;
import java.util.Set;

/**
 * Wrapper around the {@link CacheServiceLoader} to provide an interface for handling
 * the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView}s for a Gaffer graph.
 */
public class NamedViewCache {

    private static final String CACHE_NAME = "NamedView";

    /**
     * Adds the supplied {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} to the cache.  If the overwrite flag is set to false, and the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} already exists,
     * the Exception thrown will include an overwrite message.  Otherwise, the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} with the same name will simply be overwritten.
     * If it turns out the user is overwriting a non-existent {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail}, then the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} will be added normally.
     *
     * @param namedViewDetail The {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} to store
     * @param overwrite Flag relating to whether the user is adding (false) or updating/overwriting (true).
     * @throws CacheOperationFailedException if the add operation fails.
     */
    public void addNamedView(final NamedViewDetail namedViewDetail, final boolean overwrite) throws CacheOperationFailedException {
        if (null != namedViewDetail.getName()) {
            namedViewDetail.getName();
        } else {
            throw new IllegalArgumentException("NamedView name cannot be null");
        }

        if (!overwrite) {
            addToCache(namedViewDetail, false);
        } else {
            addToCache(namedViewDetail, true);
        }
    }

    /**
     * Removes the specified {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} from the cache.
     *
     * @param name {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} name to delete
     * @throws CacheOperationFailedException if the remove operation fails
     */
    public void deleteNamedView(final String name) throws CacheOperationFailedException {
        if (null != name) {
            deleteFromCache(name);
        } else {
            throw new IllegalArgumentException("NamedView name cannot be null");
        }
    }

    /**
     * Gets the specified {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} from the cache.
     *
     * @param name {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} name to get
     * @return namedView {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} of specified name
     * @throws CacheOperationFailedException if the get operation fails
     */
    public NamedViewDetail getNamedView(final String name) throws CacheOperationFailedException {
        if (null != name) {
            return getFromCache(name);
        } else {
            throw new IllegalArgumentException("NamedView name cannot be null");
        }
    }

    /**
     * Gets all the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail}s from the cache.
     *
     * @return a {@link CloseableIterable} containing all of the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail}s in the cache
     * @throws CacheOperationFailedException if the get operation fails
     */
    public CloseableIterable<NamedViewDetail> getAllNamedViews() throws CacheOperationFailedException {
        final Set<String> keys = CacheServiceLoader.getService().getAllKeysFromCache(CACHE_NAME);
        final Set<NamedViewDetail> views = new HashSet<>();
        for (final String key : keys) {
            try {
                views.add(getFromCache(key));
            } catch (final CacheOperationFailedException e) {
                throw e;
            }
        }
        return new WrappedCloseableIterable<>(views);
    }

    /**
     * Clear the {@code NamedViewCache}.
     *
     * @throws CacheOperationFailedException if there was an error clearing the cache
     */
    public void clearCache() throws CacheOperationFailedException {
        try {
            CacheServiceLoader.getService().clearCache(CACHE_NAME);
        } catch (final CacheOperationException e) {
            throw new CacheOperationFailedException("Failed to clear cache", e);
        }
    }

    /**
     * Delete the specified {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} from the cache.
     *
     * @param name the name of the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} to delete
     * @throws CacheOperationFailedException if the remove operation fails
     */
    public void deleteFromCache(final String name) throws CacheOperationFailedException {
        CacheServiceLoader.getService().removeFromCache(CACHE_NAME, name);

        if (null != CacheServiceLoader.getService().getFromCache(CACHE_NAME, name)) {
            throw new CacheOperationFailedException("Failed to remove " + name + " from cache");
        }
    }

    /**
     * Add the specified {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} to the cache
     *
     * @param namedView the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} to add to the cache
     * @param overwrite if true, overwrite any existing entry which matches the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} name
     * @throws CacheOperationFailedException if the add operation fails
     */
    public void addToCache(final NamedViewDetail namedView, final boolean overwrite) throws CacheOperationFailedException {
        try {
            if (overwrite) {
                CacheServiceLoader.getService().putInCache(CACHE_NAME, namedView.getName(), namedView);
            } else {
                CacheServiceLoader.getService().putSafeInCache(CACHE_NAME, namedView.getName(), namedView);
            }
        } catch (final CacheOperationException e) {
            throw new CacheOperationFailedException(e);
        }
    }

    /**
     * Get the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} related to the specified name from cache
     *
     * @param name the name of the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} to return
     * @return the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail}
     * @throws CacheOperationFailedException if the get operation fails, or the name does not exist in cache
     */
    public NamedViewDetail getFromCache(final String name) throws CacheOperationFailedException {
        if (null != name) {
            final NamedViewDetail namedViewFromCache = CacheServiceLoader.getService().getFromCache(CACHE_NAME, name);
            if (null != namedViewFromCache) {
                return namedViewFromCache;
            } else {
                throw new CacheOperationFailedException("No NamedViewDetail with the name " + name + " exists in the cache");
            }
        } else {
            throw new CacheOperationFailedException("NamedView name cannot be null");
        }
    }
}
