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
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView;
import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;

import java.util.HashSet;
import java.util.Set;

/**
 * /**
 * Wrapper around the {@link CacheServiceLoader} to provide an interface for handling
 * the {@link NamedView}s for a Gaffer graph.
 */
public class NamedViewCache {

    private static final String CACHE_NAME = "NamedView";

    public void addNamedView(final NamedView namedView, final boolean overwrite) throws CacheOperationFailedException {
        if (null != namedView.getViewName()) {
            namedView.getViewName();
        } else {
            throw new IllegalArgumentException("NamedView name cannot be null");
        }

        if (!overwrite) {
            addToCache(namedView, false);
        } else {
            addToCache(namedView, true);
        }
    }

    public void deleteNamedView(final String name) throws CacheOperationFailedException {
        if (null != name) {
            deleteFromCache(name);
        } else {
            throw new IllegalArgumentException("NamedView name cannot be null");
        }
    }

    public NamedView getNamedView(final String name) throws CacheOperationFailedException {
        if (null != name) {
            return getFromCache(name);
        } else {
            throw new IllegalArgumentException("NamedView name cannot be null");
        }
    }

    public CloseableIterable<NamedView> getAllNamedViews() throws CacheOperationFailedException {
        final Set<String> keys = CacheServiceLoader.getService().getAllKeysFromCache(CACHE_NAME);
        final Set<NamedView> views = new HashSet<>();
        for (final String key : keys) {
            try {
                views.add(getFromCache(key));
            } catch (final CacheOperationFailedException e) {
                throw e;
            }
        }
        return new WrappedCloseableIterable<>(views);
    }

    public void clearCache() throws CacheOperationFailedException {
        try {
            CacheServiceLoader.getService().clearCache(CACHE_NAME);
        } catch (final CacheOperationException e) {
            throw new CacheOperationFailedException("Failed to clear cache", e);
        }
    }

    public void deleteFromCache(final String name) throws CacheOperationFailedException {
        CacheServiceLoader.getService().removeFromCache(CACHE_NAME, name);

        if (null != CacheServiceLoader.getService().getFromCache(CACHE_NAME, name)) {
            throw new CacheOperationFailedException("Failed to remove " + name + " from cache");
        }
    }

    public void addToCache(final NamedView operation, final boolean overwrite) throws CacheOperationFailedException {
        try {
            if (overwrite) {
                CacheServiceLoader.getService().putInCache(CACHE_NAME, operation.getViewName(), operation);
            } else {
                CacheServiceLoader.getService().putSafeInCache(CACHE_NAME, operation.getViewName(), operation);
            }
        } catch (final CacheOperationException e) {
            throw new CacheOperationFailedException(e);
        }
    }

    public NamedView getFromCache(final String name) throws CacheOperationFailedException {
        if (null != name) {
            final NamedView namedViewFromCache = CacheServiceLoader.getService().getFromCache(CACHE_NAME, name);
            if (null != namedViewFromCache) {
                return namedViewFromCache;
            } else {
                throw new CacheOperationFailedException("No named view with the name " + name + " exists in the cache");
            }
        } else {
            throw new CacheOperationFailedException("Operation name cannot be null");
        }
    }
}
