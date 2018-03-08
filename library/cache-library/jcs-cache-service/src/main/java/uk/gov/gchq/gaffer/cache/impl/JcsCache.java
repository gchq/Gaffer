/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.cache.impl;

import org.apache.commons.jcs.JCS;
import org.apache.commons.jcs.access.GroupCacheAccess;
import org.apache.commons.jcs.access.exception.CacheException;
import org.apache.commons.jcs.engine.behavior.ICompositeCacheAttributes;
import org.apache.commons.jcs.engine.control.CompositeCache;

import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

/**
 * Implementation of the {@link ICache} interface, using a JCS {@link GroupCacheAccess}
 * object as the cache data store.
 *
 * @param <K> The object type that acts as the key for the cache
 * @param <V> The value that is stored in the cache
 */
public class JcsCache <K, V> implements ICache<K, V> {

    private final GroupCacheAccess<K, V> cache;
    private final String groupName;

    public JcsCache(final CompositeCache cache) throws CacheException {
        this(cache.getCacheName(), cache.getCacheAttributes());
    }

    private JcsCache(final String cacheName, final ICompositeCacheAttributes attr) throws CacheException {
        this.groupName = cacheName;
        this.cache = JCS.getGroupCacheInstance(cacheName, attr);
    }

    @Override
    public V get(final K key) {
        return cache.getFromGroup(key, groupName);
    }

    @Override
    public void put(final K key, final V value) throws CacheOperationException {
        if (null == key) {
            throw new CacheOperationException("Key must not be null");
        }
        try {
            cache.putInGroup(key, groupName, value);
        } catch (final CacheException e) {
            throw new CacheOperationException("Failed to add item to cache", e);
        }
    }

    @Override
    public void remove(final K key) {
        cache.removeFromGroup(key, groupName);
    }

    @Override
    public Collection<V> getAllValues() {
        ArrayList<V> values = new ArrayList<V>();
        Set<K> keys = getAllKeys();

        for (final K key : keys) {
            values.add(get(key));
        }

        return values;
    }

    @Override
    public Set<K> getAllKeys() {
        return cache.getGroupKeys(groupName);
    }

    @Override
    public int size() {
        return getAllKeys().size();
    }

    @Override
    public void clear() throws CacheOperationException {
        try {
            cache.clear();
        } catch (final CacheException e) {
            throw new CacheOperationException("Failed to clear cache", e);
        }
    }
}
