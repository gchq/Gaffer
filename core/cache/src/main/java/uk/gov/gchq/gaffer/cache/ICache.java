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

import java.util.Collection;
import java.util.Set;

/**
 * Interface that All uk.gov.gchq.gaffer.cache objects must abide by so components may instantiate any implementation of a uk.gov.gchq.gaffer.cache - no
 * matter what uk.gov.gchq.gaffer.cache it is.
 *
 * @param <K> The object type that acts as the key for the uk.gov.gchq.gaffer.cache
 * @param <V> The value that is stored in the uk.gov.gchq.gaffer.cache
 */
public interface ICache <K, V> {

    V get(final K key);

    void put(final K key, final V value) throws CacheOperationException;

    void putSafe(final K key, final V value) throws CacheOperationException;

    void remove(final K key);

    Collection<V> getAllValues();

    Set<K> getAllKeys();

    int size();

    void clear() throws CacheOperationException;

}
