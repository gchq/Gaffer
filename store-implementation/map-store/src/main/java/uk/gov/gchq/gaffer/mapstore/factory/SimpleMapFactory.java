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
import uk.gov.gchq.gaffer.mapstore.multimap.MapOfSets;
import uk.gov.gchq.gaffer.mapstore.multimap.MultiMap;
import uk.gov.gchq.gaffer.mapstore.utils.ElementCloner;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameIdResolver;

import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation of the {@link MapFactory} interface, used to create map
 * instances for the {@link uk.gov.gchq.gaffer.mapstore.MapStore}.
 */
public class SimpleMapFactory implements MapFactory {
    public static final String MAP_CLASS = "gaffer.store.mapstore.map.class";
    public static final String MAP_CLASS_DEFAULT = HashMap.class.getName();

    private final ElementCloner cloner;
    private Class<? extends Map> mapClass = HashMap.class;

    private Map<String, Map> maps = new HashMap<>();
    private Map<String, MultiMap> multiMaps = new HashMap<>();

    public SimpleMapFactory() {
        this(new ElementCloner());
    }

    protected SimpleMapFactory(final ElementCloner cloner) {
        this.cloner = cloner;
    }

    @Override
    public void initialise(final Schema schema, final MapStoreProperties properties) {
        final String mapClassName = properties.get(MAP_CLASS, MAP_CLASS_DEFAULT);
        try {
            mapClass = Class.forName(SimpleClassNameIdResolver.getClassName(mapClassName)).asSubclass(Map.class);
        } catch (final ClassNotFoundException | ClassCastException e) {
            throw new IllegalArgumentException("Map Class is invalid: " + mapClassName, e);
        }
    }

    @Override
    public <K, V> Map<K, V> getMap(final String mapName, final Class<K> keyClass, final Class<V> valueClass) {
        Map map = maps.get(mapName);
        if (null == map) {
            try {
                map = mapClass.newInstance();
            } catch (final InstantiationException | IllegalAccessException e) {
                throw new IllegalArgumentException("Unable to create new map instance of type: " + mapClass.getName());
            }
            maps.put(mapName, map);
        }
        return map;
    }

    @Override
    public <K, V> MultiMap<K, V> getMultiMap(final String mapName, final Class<K> keyClass, final Class<V> valueClass) {
        return multiMaps.computeIfAbsent(mapName, n -> new MapOfSets(getMap(n, keyClass, valueClass)));
    }

    @Override
    public void clear() {
        maps.clear();
        multiMaps.clear();
    }

    @Override
    public Element cloneElement(final Element element, final Schema schema) {
        return cloner.cloneElement(element, schema);
    }

    protected Class<? extends Map> getMapClass() {
        return mapClass;
    }
}
