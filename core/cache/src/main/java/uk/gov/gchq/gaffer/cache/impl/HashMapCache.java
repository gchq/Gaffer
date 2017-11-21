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

package uk.gov.gchq.gaffer.cache.impl;

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.cache.ICache;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.implementation.JavaSerialiser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

/**
 * Simple implementation of the {@link ICache} interface, using a {@link HashMap}
 * as the cache data store.
 *
 * @param <K> The object type that acts as the key for the HashMap
 * @param <V> The value that is stored in the HashMap
 */
public class HashMapCache<K, V> implements ICache<K, V> {
    private static final JavaSerialiser JAVA_SERIALISER = new JavaSerialiser();
    private boolean useJavaSerialisation;
    private HashMap<K, Object> cache = new HashMap<>();

    public HashMapCache(final boolean useJavaSerialisation) {
        this.useJavaSerialisation = useJavaSerialisation;
    }

    public HashMapCache() {
        this(false);
    }

    @Override
    public V get(final K key) {
        try {
            return (V) (useJavaSerialisation
                    ? JAVA_SERIALISER.deserialise((byte[]) cache.get(key))
                    : cache.get(key));
        } catch (final SerialisationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(final K key, final V value) {
        if (useJavaSerialisation) {
            try {
                cache.put(key, JAVA_SERIALISER.serialise(value));
            } catch (final SerialisationException e) {
                throw new RuntimeException(e);
            }
        } else {
            cache.<K, V>put(key, value);
        }
    }

    @Override
    public void remove(final K key) {
        cache.remove(key);
    }

    @Override
    public Collection<V> getAllValues() {
        ArrayList<V> rtn = Lists.newArrayList();
        if (useJavaSerialisation) {
            cache.values()
                    .forEach((Object o) -> {
                        try {
                            rtn.add((V) JAVA_SERIALISER.deserialise((byte[]) o));
                        } catch (final SerialisationException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } else {
            rtn.addAll((Collection<V>) cache.values());
        }
        return rtn;
    }

    @Override
    public Set<K> getAllKeys() {
        return cache.keySet();
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public void clear() {
        cache.clear();
    }
}
