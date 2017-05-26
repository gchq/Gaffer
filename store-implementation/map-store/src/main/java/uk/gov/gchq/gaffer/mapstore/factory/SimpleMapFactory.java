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
package uk.gov.gchq.gaffer.mapstore.factory;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.mapstore.multimap.MapOfSets;
import uk.gov.gchq.gaffer.mapstore.multimap.MultiMap;
import uk.gov.gchq.gaffer.mapstore.utils.ElementCloner;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.HashMap;
import java.util.Map;

public class SimpleMapFactory implements MapFactory {
    private Class<? extends Map> mapClass = HashMap.class;

    @Override
    public void initialise(final MapStoreProperties properties) {
        try {
            mapClass = Class.forName(properties.getMapClass()).asSubclass(Map.class);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Map Class is invalid: " + properties.getMapClass(), e);
        }
    }

    @Override
    public <K, V> Map<K, V> newMap(final String mapName) {
        try {
            return mapClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException("Unable to create new map instance of type: " + mapClass.getName());
        }
    }

    @Override
    public void clear() {
        // no action required
    }

    @Override
    public <K, V> MultiMap<K, V> newMultiMap(final String mapName) {
        return new MapOfSets<>(newMap(mapName));
    }

    @Override
    public Element cloneElement(final Element element, final Schema schema) {
        return ElementCloner.cloneElement(element, schema);
    }
}
