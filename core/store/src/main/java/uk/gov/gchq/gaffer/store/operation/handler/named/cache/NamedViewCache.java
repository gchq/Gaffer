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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;
import uk.gov.gchq.gaffer.user.User;

/**
 * /**
 * Wrapper around the {@link uk.gov.gchq.gaffer.cache.CacheServiceLoader} to provide an interface for handling
 * the {@link uk.gov.gchq.gaffer.data.elementdefinition.NamedView}s for a Gaffer graph.
 */
public class NamedViewCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(NamedViewCache.class);
    private static final String CACHE_NAME = "NamedView";

    public void addNamedView(final NamedView namedView, final boolean overwrite, final User user) throws CacheOperationFailedException {
        String name;
        try {
            name = namedView.getViewName();
        } catch (final NullPointerException e) {
            throw new CacheOperationFailedException("NamedOperation cannot be null", e);
        }
        if (null == name) {
            throw new CacheOperationFailedException("NamedOperation name cannot be null");
        }
        if (!overwrite) {
            addToCache(name, namedView, false);
            return;
        }

        NamedView existing = null;

        try {
            existing = getFromCache(name);
        } catch (final CacheOperationFailedException e) { // if there is no existing named View add one
            addToCache(name, namedView, false);
            return;
        }
        if (existing.hasWriteAccess(user)) {
            addToCache(name, namedView, true);
        } else {
            throw new CacheOperationFailedException("User " + namedView.getCreatorId() + " does not have permission to overwrite");
        }
    }
}
