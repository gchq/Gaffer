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

    static final String COUNT = "COUNT";

    // elementToProperties maps from an Element containing the group-by properties to a Properties object without the
    // group-by properties
    Map<Element, Properties> elementToProperties;
    // entityIdToElements is a map from an EntityId to the element key from elementToProperties
    MultiMap<EntityId, Element> entityIdToElements;
    // edgeIdToElements is a map from an EdgeId to the element key from elementToProperties
    MultiMap<EdgeId, Element> edgeIdToElements;
    final boolean maintainIndex;
    final Map<String, Set<String>> groupToGroupByProperties = new HashMap<>();
    final Map<String, Set<String>> groupToNonGroupByProperties = new HashMap<>();
    final Set<String> groupsWithNoAggregation = new HashSet<>();
    final Schema schema;
    final MapFactory mapFactory;

    public MapImpl(final Schema schema, final MapStoreProperties mapStoreProperties) throws StoreException {
        final MapFactory mapFactoryTmp = mapStoreProperties.getMapFactory();
        if (null == mapFactoryTmp) {
            mapFactory = new SimpleMapFactory();
        } else {
            mapFactory = mapFactoryTmp;
        }

        mapFactory.initialise(mapStoreProperties);
        maintainIndex = mapStoreProperties.getCreateIndex();
        elementToProperties = mapFactory.newMap("elementToProperties");
        if (maintainIndex) {
            entityIdToElements = mapFactory.newMultiMap("entityIdToElements");
            edgeIdToElements = mapFactory.newMultiMap("edgeIdToElements");
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
