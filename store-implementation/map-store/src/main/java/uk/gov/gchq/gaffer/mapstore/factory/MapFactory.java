/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.mapstore.factory;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.mapstore.multimap.MultiMap;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Map;

/**
 * Interface describing factory classes for creating backing map instances to be
 * used as the data store for a {@link uk.gov.gchq.gaffer.mapstore.MapStore}.
 */
public interface MapFactory {

    /**
     * Initialise the map with the specified schema and properties.
     *
     * @param schema the schema to apply
     * @param properties the store properties to apply
     */
    void initialise(final Schema schema, final MapStoreProperties properties);

    /**
     * Retrieve a named {@link Map}.
     *
     * If the requested map does not exist, a new map is created and cached.
     *
     * @param mapName the name of the map to retrieve
     * @param keyClass the class type to use for the map keys
     * @param valueClass the class type to use for the map values
     * @param <K> the type of the map keys
     * @param <V> the type of the map values
     * @return the requested {@link Map} object
     */
    <K, V> Map<K, V> getMap(final String mapName, final Class<K> keyClass, final Class<V> valueClass);

    /**
     * Retrieve a named {@link MultiMap}.
     *
     * If the requested map does not exist, a new multi map is created and cached.
     *
     * @param mapName the name of the multi map to retrieve
     * @param keyClass the class type to use for the map keys
     * @param valueClass the class type to use for the map values
     * @param <K> the type of the map keys
     * @param <V> the type of the map values
     * @return the requested {@link MultiMap} object
     */
    <K, V> MultiMap<K, V> getMultiMap(final String mapName, final Class<K> keyClass, final Class<V> valueClass);

    /**
     * Update a value associated with a key in a specified map object.
     *
     * @param map the map to update
     * @param key the key to update the corresponding value for
     * @param updatedValue the updated value
     * @param <K> the type of the map keys
     * @param <V> the type of the map values
     */
    default <K, V> void updateValue(final Map<K, V> map, final K key, final V updatedValue) {
        // no action required.
    }

    /**
     * Clear any currently configured Maps.
     */
    void clear();

    /**
     * Clone an element.
     *
     * @param element the element to clone
     * @param schema the relevant schema
     * @return the cloned element
     */
    Element cloneElement(final Element element, final Schema schema);
}
