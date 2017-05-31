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
package uk.gov.gchq.gaffer.mapstore.impl;

import com.google.common.collect.Sets;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.mapstore.factory.MapFactory;
import uk.gov.gchq.gaffer.mapstore.factory.SimpleMapFactory;
import uk.gov.gchq.gaffer.mapstore.multimap.MultiMap;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The internal variables of this class are package-private. This allows operation handlers for the
 * {@link uk.gov.gchq.gaffer.mapstore.MapStore} to be placed in the same package and get access to the maps, without
 * exposing the internal state of the MapStore to classes outside of this package.
 */
public class MapImpl {
    public static final String COUNT = "COUNT";
    public static final String ELEMENT_TO_PROPERTIES = "elementToProperties";
    public static final String ENTITY_ID_TO_ELEMENTS = "entityIdToElements";
    public static final String EDGE_ID_TO_ELEMENTS = "edgeIdToElements";
    public static final Set<String> MAP_NAMES = Collections.unmodifiableSet(
            Collections.singleton(ELEMENT_TO_PROPERTIES));
    public static final Set<String> MULTI_MAP_NAMES = Collections.unmodifiableSet(
            Sets.newHashSet(ENTITY_ID_TO_ELEMENTS, EDGE_ID_TO_ELEMENTS));

    /**
     * elementToProperties maps from an Element containing the group-by properties
     * to a Properties object without the group-by properties
     */
    final Map<Element, Properties> elementToProperties;

    /**
     * entityIdToElements is a map from an EntityId to the element key from elementToProperties
     */
    final MultiMap<EntityId, Element> entityIdToElements;

    /**
     * edgeIdToElements is a map from an EdgeId to the element key from elementToProperties
     */
    final MultiMap<EdgeId, Element> edgeIdToElements;


    final boolean maintainIndex;
    final Map<String, Set<String>> groupToGroupByProperties = new HashMap<>();
    final Map<String, Set<String>> groupToNonGroupByProperties = new HashMap<>();
    final Set<String> groupsWithNoAggregation = new HashSet<>();
    final Schema schema;
    final MapFactory mapFactory;

    public MapImpl(final Schema schema, final MapStoreProperties mapStoreProperties) throws StoreException {
        mapFactory = createMapFactory(mapStoreProperties);
        maintainIndex = mapStoreProperties.getCreateIndex();
        elementToProperties = mapFactory.getMap(ELEMENT_TO_PROPERTIES);
        if (maintainIndex) {
            entityIdToElements = mapFactory.getMultiMap(ENTITY_ID_TO_ELEMENTS);
            edgeIdToElements = mapFactory.getMultiMap(EDGE_ID_TO_ELEMENTS);
        } else {
            entityIdToElements = null;
            edgeIdToElements = null;
        }
        this.schema = schema;
        schema.getEntityGroups().forEach(g -> addToGroupByMap(this.schema, g));
        schema.getEdgeGroups().forEach(g -> addToGroupByMap(this.schema, g));
    }

    public void clear() {
        elementToProperties.clear();
        groupToGroupByProperties.clear();
        groupToNonGroupByProperties.clear();
        groupsWithNoAggregation.clear();
        if (maintainIndex) {
            entityIdToElements.clear();
            edgeIdToElements.clear();
        }

        mapFactory.clear();
    }

    protected MapFactory createMapFactory(final MapStoreProperties mapStoreProperties) {
        final MapFactory mapFactory;
        final String factoryClass = mapStoreProperties.getMapFactory();
        if (null == factoryClass) {
            mapFactory = new SimpleMapFactory();
        } else {
            try {
                mapFactory = Class.forName(factoryClass).asSubclass(MapFactory.class).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new IllegalArgumentException("MapFactory is invalid: " + factoryClass, e);
            }
        }

        mapFactory.initialise(mapStoreProperties);
        return mapFactory;
    }

    private void addToGroupByMap(final Schema schema, final String group) {
        final SchemaElementDefinition sed = schema.getElement(group);
        groupToGroupByProperties.put(group, sed.getGroupBy());
        if (null == sed.getGroupBy() || sed.getGroupBy().isEmpty()) {
            groupsWithNoAggregation.add(group);
        }
        final Set<String> nonGroupByProperties = new HashSet<>(sed.getProperties());
        nonGroupByProperties.removeAll(sed.getGroupBy());
        groupToNonGroupByProperties.put(group, nonGroupByProperties);
    }
}
