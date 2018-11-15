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
package uk.gov.gchq.gaffer.mapstore.impl;

import uk.gov.gchq.gaffer.commonutil.iterable.RepeatItemIterable;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
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
import uk.gov.gchq.gaffer.store.util.AggregatorUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Map data store implementation use by the Gaffer {@link uk.gov.gchq.gaffer.mapstore.MapStore}
 * class.
 *
 * This class can be thought of as an analogue to a conventional database. Internally,
 * different {@link Map} and {@link MultiMap} instances are used to keep track of
 * the stored elements and the relationships between those elements. This data store
 * is then abstracted again as a Gaffer {@link uk.gov.gchq.gaffer.store.Store} (by
 * the {@link uk.gov.gchq.gaffer.mapstore.MapStore} class) to give Gaffer-specific
 * functionality.
 *
 * The internal variables of this class are package-private. This allows operation
 * handlers for the {@link uk.gov.gchq.gaffer.mapstore.MapStore} to be placed in the
 * same package and get access to the maps, without exposing the internal state of
 * the MapStore to classes outside of this package.
 */
public class MapImpl {
    public static final String AGG_ELEMENTS = "aggElements";
    public static final String NON_AGG_ELEMENTS = "nonAggElements";
    public static final String ENTITY_ID_TO_ELEMENTS = "entityIdToElements";
    public static final String EDGE_ID_TO_ELEMENTS = "edgeIdToElements";

    /**
     * aggElements maps from an Element group to a map of Elements containing the group-by properties
     * to a Properties object without the group-by properties
     */
    private final Map<String, Map<Element, GroupedProperties>> aggElements = new HashMap<>();

    /**
     * nonAggElements maps from an Element group to a map of non aggregated Elements to the count of the
     * number of times that element has been seen.
     */
    private final Map<String, Map<Element, Long>> nonAggElements = new HashMap<>();

    /**
     * entityIdToElements is a map from an EntityId to the element key from aggElements or nonAggElements
     */
    private final MultiMap<EntityId, Element> entityIdToElements;

    /**
     * edgeIdToElements is a map from an EdgeId to the element key from aggElements or nonAggElements
     */
    private final MultiMap<EdgeId, Element> edgeIdToElements;

    private final MapFactory mapFactory;
    private final Map<String, Set<String>> groupToGroupByProperties = new HashMap<>();
    private final Map<String, Set<String>> groupToNonGroupByProperties = new HashMap<>();
    private final Set<String> groupsWithNoAggregation = new HashSet<>();
    private final List<String> aggregatedGroups;
    private final Schema schema;
    private final boolean maintainIndex;
    private final AggregatorUtil.IngestPropertiesBinaryOperator propertyAggregator;

    public MapImpl(final Schema schema, final MapStoreProperties mapStoreProperties) {
        this.schema = schema;
        propertyAggregator = new AggregatorUtil.IngestPropertiesBinaryOperator(schema);
        mapFactory = createMapFactory(schema, mapStoreProperties);
        maintainIndex = mapStoreProperties.getCreateIndex();

        for (final String group : schema.getGroups()) {
            aggElements.put(group, mapFactory.getMap(group + "|" + AGG_ELEMENTS, Element.class, GroupedProperties.class));
            nonAggElements.put(group, mapFactory.getMap(group + "|" + NON_AGG_ELEMENTS, Element.class, Long.class));
        }

        if (maintainIndex) {
            entityIdToElements = mapFactory.getMultiMap(ENTITY_ID_TO_ELEMENTS, EntityId.class, Element.class);
            edgeIdToElements = mapFactory.getMultiMap(EDGE_ID_TO_ELEMENTS, EdgeId.class, Element.class);
        } else {
            entityIdToElements = null;
            edgeIdToElements = null;
        }

        this.aggregatedGroups = schema.getAggregatedGroups();
        schema.getEntityGroups().forEach(this::addToGroupByMap);
        schema.getEdgeGroups().forEach(this::addToGroupByMap);
    }

    public void clear() {
        aggElements.clear();
        nonAggElements.clear();
        if (maintainIndex) {
            entityIdToElements.clear();
            edgeIdToElements.clear();
        }
    }

    void addNonAggElement(final Element element) {
        nonAggElements.get(element.getGroup()).merge(element, 1L, (a, b) -> a + b);
    }

    void addAggElement(final Element elementWithGroupByProperties, final GroupedProperties properties) {
        aggElements.get(elementWithGroupByProperties.getGroup())
                .merge(elementWithGroupByProperties, properties, propertyAggregator);
    }

    Collection<Element> lookup(final EntityId entitId) {
        Collection<Element> results = entityIdToElements.get(entitId);
        if (null == results) {
            results = Collections.emptySet();
        }

        return results;
    }

    Collection<Element> lookup(final EdgeId edgeId) {
        Collection<Element> results = edgeIdToElements.get(edgeId);
        if (null == results) {
            results = Collections.emptySet();
        }

        return results;
    }

    Iterable<Element> getNonAggElements(final Element element) {
        final Long count = nonAggElements.get(element.getGroup()).get(element);
        if (null == count || count < 1) {
            return Collections.emptyList();
        }
        return new RepeatItemIterable<>(element, count);
    }

    Element getAggElement(final Element element) {
        final Element clone = element.emptyClone();
        clone.copyProperties(element.getProperties());
        clone.copyProperties(aggElements.get(element.getGroup()).get(element));
        return clone;
    }

    Iterable<Element> getElements(final Element element) {
        if (!isAggregationEnabled(element)) {
            return getNonAggElements(element);
        } else {
            return Collections.singletonList(getAggElement(element));
        }
    }

    Stream<Element> getAllAggElements(final Set<String> groups) {
        return aggElements.entrySet().stream()
                .filter(entry -> groups.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .flatMap(map -> map.entrySet().stream())
                .map(x -> {
                    final Element element = x.getKey().emptyClone();
                    element.copyProperties(x.getKey().getProperties());
                    element.copyProperties(x.getValue());
                    return cloneElement(element, schema);
                });
    }

    Stream<Element> getAllNonAggElements(final Set<String> groups) {
        return nonAggElements.entrySet().stream()
                .filter(entry -> groups.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .flatMap(map -> map.entrySet().stream())
                .map(x -> new RepeatItemIterable<>(cloneElement(x.getKey(), schema), x.getValue()))
                .flatMap(Streams::toStream);
    }

    Stream<Element> getAllElements(final Set<String> groups) {
        return Stream.concat(getAllAggElements(groups), getAllNonAggElements(groups));
    }

    void addIndex(final EntityId entityId, final Element element) {
        entityIdToElements.put(entityId, element);
    }

    void addIndex(final EdgeId edgeId, final Element element) {
        edgeIdToElements.put(edgeId, element);
    }

    boolean isMaintainIndex() {
        return maintainIndex;
    }

    Element cloneElement(final Element element, final Schema schema) {
        return mapFactory.cloneElement(element, schema);
    }

    Set<String> getGroupByProperties(final String group) {
        return groupToGroupByProperties.get(group);
    }

    Set<String> getNonGroupByProperties(final String group) {
        return groupToNonGroupByProperties.get(group);
    }

    boolean isAggregationEnabled(final Element element) {
        return !groupsWithNoAggregation.contains(element.getGroup());
    }

    long countAggElements() {
        long totalCount = 0;
        for (final Map<Element, GroupedProperties> map : aggElements.values()) {
            totalCount += map.size();
        }

        return totalCount;
    }

    long countNonAggElements() {
        long totalCount = 0;
        for (final Map<Element, Long> map : nonAggElements.values()) {
            for (final Long count : map.values()) {
                if (null != count) {
                    totalCount += count;
                }
            }
        }

        return totalCount;
    }

    private MapFactory createMapFactory(final Schema schema, final MapStoreProperties mapStoreProperties) {
        final MapFactory mapFactory;
        final String factoryClass = mapStoreProperties.getMapFactory();
        if (null == factoryClass) {
            mapFactory = new SimpleMapFactory();
        } else {
            try {
                mapFactory = Class.forName(factoryClass).asSubclass(MapFactory.class).newInstance();
            } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new IllegalArgumentException("MapFactory is invalid: " + factoryClass, e);
            }
        }

        mapFactory.initialise(schema, mapStoreProperties);
        return mapFactory;
    }

    private void addToGroupByMap(final String group) {
        final SchemaElementDefinition sed = schema.getElement(group);
        groupToGroupByProperties.put(group, sed.getGroupBy());
        if (!aggregatedGroups.contains(group)) {
            groupsWithNoAggregation.add(group);
        }
        final Set<String> nonGroupByProperties = new HashSet<>(sed.getProperties());
        nonGroupByProperties.removeAll(sed.getGroupBy());
        groupToNonGroupByProperties.put(group, nonGroupByProperties);
    }
}
