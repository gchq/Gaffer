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
import uk.gov.gchq.gaffer.data.element.GroupedProperties;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.mapstore.factory.MapFactory;
import uk.gov.gchq.gaffer.mapstore.factory.SimpleMapFactory;
import uk.gov.gchq.gaffer.mapstore.multimap.MultiMap;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The internal variables of this class are package-private. This allows operation handlers for the
 * {@link uk.gov.gchq.gaffer.mapstore.MapStore} to be placed in the same package and get access to the maps, without
 * exposing the internal state of the MapStore to classes outside of this package.
 */
public class MapImpl {
    public static final String AGG_ELEMENTS = "aggElements";
    public static final String NON_AGG_ELEMENTS = "nonAggElements";
    public static final String ENTITY_ID_TO_ELEMENTS = "entityIdToElements";
    public static final String EDGE_ID_TO_ELEMENTS = "edgeIdToElements";
    public static final Set<String> MAP_NAMES = Collections.unmodifiableSet(
            Sets.newHashSet(AGG_ELEMENTS, NON_AGG_ELEMENTS));
    public static final Set<String> MULTI_MAP_NAMES = Collections.unmodifiableSet(
            Sets.newHashSet(ENTITY_ID_TO_ELEMENTS, EDGE_ID_TO_ELEMENTS));

    /**
     * aggElements maps from an Element containing the group-by properties
     * to a Properties object without the group-by properties
     */
    final Map<String, Map<Element, GroupedProperties>> aggElements = new HashMap<>();

    /**
     * nonAggElements maps from a non aggregated Element to the count of the
     * number of times that element has been seen.
     */
    final Map<String, Map<Element, Integer>> nonAggElements = new HashMap<>();

    /**
     * entityIdToElements is a map from an EntityId to the element key from aggElements
     */
    final MultiMap<EntityId, Element> entityIdToElements;

    /**
     * edgeIdToElements is a map from an EdgeId to the element key from aggElements
     */
    final MultiMap<EdgeId, Element> edgeIdToElements;

    final MapFactory mapFactory;
    final boolean maintainIndex;

    private final Map<String, Set<String>> groupToGroupByProperties = new HashMap<>();
    private final Map<String, Set<String>> groupToNonGroupByProperties = new HashMap<>();
    private final Set<String> groupsWithNoAggregation = new HashSet<>();
    final List<String> aggregatedGroups;
    private final Schema schema;

    public MapImpl(final Schema schema) {
        this(schema, new MapStoreProperties());
    }

    public MapImpl(final Schema schema, final MapStoreProperties mapStoreProperties) {
        mapFactory = createMapFactory(schema, mapStoreProperties);
        maintainIndex = mapStoreProperties.getCreateIndex();

        for (final String group : schema.getGroups()) {
            aggElements.put(group, mapFactory.getMap(group + "|" + AGG_ELEMENTS, Element.class, GroupedProperties.class));
            nonAggElements.put(group, mapFactory.getMap(group + "|" + NON_AGG_ELEMENTS, Element.class, Integer.class));
        }

        if (maintainIndex) {
            entityIdToElements = mapFactory.getMultiMap(ENTITY_ID_TO_ELEMENTS, EntityId.class, Element.class);
            edgeIdToElements = mapFactory.getMultiMap(EDGE_ID_TO_ELEMENTS, EdgeId.class, Element.class);
        } else {
            entityIdToElements = null;
            edgeIdToElements = null;
        }
        this.schema = schema;

        this.aggregatedGroups = schema.getAggregatedGroups();
        schema.getEntityGroups().forEach(this::addToGroupByMap);
        schema.getEdgeGroups().forEach(this::addToGroupByMap);
    }

    protected Set<String> getGroupByProperties(final String group) {
        return groupToGroupByProperties.get(group);
    }

    protected Set<String> getNonGroupByProperties(final String group) {
        return groupToNonGroupByProperties.get(group);
    }

    public void clear() {
        aggElements.clear();
        nonAggElements.clear();
        if (maintainIndex) {
            entityIdToElements.clear();
            edgeIdToElements.clear();
        }
    }

    protected boolean isAggregationEnabled(final Element element) {
        return !groupsWithNoAggregation.contains(element.getGroup());
    }

    protected MapFactory createMapFactory(final Schema schema, final MapStoreProperties mapStoreProperties) {
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

        mapFactory.initialise(schema, mapStoreProperties);
        return mapFactory;
    }

    private void addToGroupByMap(final String group) {
        final SchemaElementDefinition sed = schema.getElement(group);
        groupToGroupByProperties.put(group, sed.getGroupBy());
        if (null == sed.getGroupBy() || sed.getGroupBy().isEmpty() || !aggregatedGroups.contains(group)) {
            groupsWithNoAggregation.add(group);
        }
        final Set<String> nonGroupByProperties = new HashSet<>(sed.getProperties());
        nonGroupByProperties.removeAll(sed.getGroupBy());
        groupToNonGroupByProperties.put(group, nonGroupByProperties);
    }

    public void update(final MapImpl mapImpl) {
        for (final Map.Entry<String, Map<Element, GroupedProperties>> groupAndMap : mapImpl.aggElements.entrySet()) {
            final String group = groupAndMap.getKey();
            final Map<Element, GroupedProperties> existingMap = aggElements.get(group);
            if (existingMap.isEmpty()) {
                existingMap.putAll(groupAndMap.getValue());
            } else {
                for (final Map.Entry<Element, GroupedProperties> entry : groupAndMap.getValue().entrySet()) {
                    final GroupedProperties existingProperties = existingMap.get(entry.getKey());
                    if (null == existingProperties) {
                        existingMap.put(entry.getKey(), entry.getValue());
                    } else {
                        final SchemaElementDefinition elementDef = schema.getElement(existingProperties.getGroup());
                        elementDef.getAggregator().apply(existingProperties, entry.getValue());
                        existingMap.put(entry.getKey(), existingProperties);
                    }
                }
            }
        }

        for (final Map.Entry<String, Map<Element, Integer>> groupAndMap : mapImpl.nonAggElements.entrySet()) {
            final String group = groupAndMap.getKey();
            final Map<Element, Integer> existingMap = nonAggElements.get(group);
            if (existingMap.isEmpty()) {
                existingMap.putAll(groupAndMap.getValue());
            } else {
                for (final Map.Entry<Element, Integer> entry : groupAndMap.getValue().entrySet()) {
                    existingMap.merge(entry.getKey(), entry.getValue(), (a, b) -> a + b);
                }
            }
        }

        if (maintainIndex) {
            entityIdToElements.putAll(mapImpl.entityIdToElements);
            edgeIdToElements.putAll(mapImpl.edgeIdToElements);
        }
    }
}
