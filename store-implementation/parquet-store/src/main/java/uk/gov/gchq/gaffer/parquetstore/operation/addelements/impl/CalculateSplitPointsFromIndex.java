/*
 * Copyright 2017. Crown Copyright
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
package uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl;

import org.apache.spark.api.java.JavaRDD;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.index.ColumnIndex;
import uk.gov.gchq.gaffer.parquetstore.index.GraphIndex;
import uk.gov.gchq.gaffer.parquetstore.index.GroupIndex;
import uk.gov.gchq.gaffer.parquetstore.index.MinValuesWithPath;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl.RDD.CalculateSplitPointsFromJavaRDD;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Generates the split points from the {@link GraphIndex} and uses the min values per file as the split points
 */
public final class CalculateSplitPointsFromIndex {

    private CalculateSplitPointsFromIndex() {
    }

    public static Map<String, Map<Integer, Object>> apply(final GraphIndex index, final SchemaUtils schemaUtils,
                                                          final ParquetStoreProperties properties,
                                                          final Iterable<? extends Element> data) throws SerialisationException {
        final Map<String, Map<Integer, Object>> groupToSplitPoints = calculateSplitPointsFromIndex(index, schemaUtils);
        for (final String group : schemaUtils.getEntityGroups()) {
            if (!groupToSplitPoints.containsKey(group)) {
                groupToSplitPoints.put(group, new CalculateSplitPointsFromIterable(properties.getSampleRate(), properties.getAddElementsOutputFilesPerGroup() - 1).calculateSplitsForGroup(data, group, true));
            }
        }
        for (final String group : schemaUtils.getEdgeGroups()) {
            if (!groupToSplitPoints.containsKey(group)) {
                groupToSplitPoints.put(group, new CalculateSplitPointsFromIterable(properties.getSampleRate(), properties.getAddElementsOutputFilesPerGroup() - 1).calculateSplitsForGroup(data, group, false));
            }
        }
        return groupToSplitPoints;
    }

    public static Map<String, Map<Integer, Object>> apply(final GraphIndex index, final SchemaUtils schemaUtils,
                                                          final ParquetStoreProperties properties,
                                                          final JavaRDD<Element> data) throws SerialisationException {
        final Map<String, Map<Integer, Object>> groupToSplitPoints = calculateSplitPointsFromIndex(index, schemaUtils);
        for (final String group : schemaUtils.getEntityGroups()) {
            if (!groupToSplitPoints.containsKey(group)) {
                groupToSplitPoints.put(group, new CalculateSplitPointsFromJavaRDD(properties.getSampleRate(), properties.getAddElementsOutputFilesPerGroup() - 1).calculateSplitsForGroup(data, group, true));
            }
        }
        for (final String group : schemaUtils.getEdgeGroups()) {
            if (!groupToSplitPoints.containsKey(group)) {
                groupToSplitPoints.put(group, new CalculateSplitPointsFromJavaRDD(properties.getSampleRate(), properties.getAddElementsOutputFilesPerGroup() - 1).calculateSplitsForGroup(data, group, false));
            }
        }
        return groupToSplitPoints;
    }

    private static Map<String, Map<Integer, Object>> calculateSplitPointsFromIndex(final GraphIndex index, final SchemaUtils schemaUtils) throws SerialisationException {
        final Set<String> entityGroups = schemaUtils.getEntityGroups();
        final Map<String, Map<Integer, Object>> groupToSplitPoints = new TreeMap<>();
        for (final String group : index.groupsIndexed()) {
            final GroupIndex groupIndex = index.getGroup(group);
            final GafferGroupObjectConverter converter = schemaUtils.getConverter(group);
            final String col;
            if (entityGroups.contains(group)) {
                col = ParquetStoreConstants.VERTEX;
            } else {
                col = ParquetStoreConstants.SOURCE;
            }
            final ColumnIndex columnIndex = groupIndex.getColumn(col);
            final Map<Integer, Object> splitPoints = new HashMap<>();
            final Iterator<MinValuesWithPath> indexIter = columnIndex.getIterator();
            int i = 0;
            while (indexIter.hasNext()) {
                final MinValuesWithPath minValuesWithPath = indexIter.next();
                final Object gafferObject = converter.parquetObjectsToGafferObject(col, minValuesWithPath.getMin());
                final Object split = splitPoints.get(i - 1);
                if (i == 0 || !indexIter.hasNext() || split == null || !split.equals(gafferObject)) {
                    splitPoints.put(i, gafferObject);
                }
                i++;
            }
            if (!splitPoints.isEmpty()) {
                groupToSplitPoints.put(group, splitPoints);
            }
        }
        return groupToSplitPoints;
    }
}
