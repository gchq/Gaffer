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

package uk.gov.gchq.gaffer.cache;

import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;

import java.util.Collection;
import java.util.Set;

/**
 * Interface that All cache objects must abide by so components may instantiate any implementation of a cache - no
 * matter what cache it is.
 *
 * @param <K> The object type that acts as the key for the cache
 * @param <V> The value that is stored in the cache
 */
public interface ICache<K, V> {

    /**
     * Retrieve the value associated with the given key.
     *
     * @param key the key to lookup in the cache
     * @return the value associated with the key
     */
    V get(final K key);

    /**
     * Add a new key-value pair to the cache.
     *
     * @param key   the key to add
     * @param value the value to add
     * @throws CacheOperationException if there is an error adding the new key-value pair to the cache
     */
    void put(final K key, final V value) throws CacheOperationException;

    /**
     * Add a new key-value pair to the cache, but only if there is existing entry associated with the specified key.
     *
     * @param key   the key to add
     * @param value the value to add
     * @throws CacheOperationException if there is an error adding the new key-value pair to the cache
     * @throws OverwritingException    if the specified key already exists in the cache with a non-null value
     */
    default void putSafe(final K key, final V value) throws OverwritingException, CacheOperationException {
        if (null == get(key)) {
            try {
                put(key, value);
            } catch (final CacheOperationException e) {
                throw e;
            }
        } else {
            throw new OverwritingException("Cache entry already exists for key: " + key);
        }
    }

    /**
     * Remove the entry associated with the specified key.
     *
     * @param key the key of the entry to remove
     */
    void remove(final K key);

    /**
     * Get all values present in the cache.
     *
     * @return a {@link Collection} containing all of the cache values
     */
    Collection<V> getAllValues();

    /**
     * Get all keys present in the cache.
     *
     * @return a {@link Set} containing all of the cache keys
     */
    Set<K> getAllKeys();

    /**
     * Get the size of the cache.
     *
     * @return the number of entries in the caches
     */
    default int size() {
        return getAllKeys().size();
    }

    /**
     * Remove all entries from the cache.
     *
     * @throws CacheOperationException if there was an error clearing the cache
     */
    void clear() throws CacheOperationException;

}
