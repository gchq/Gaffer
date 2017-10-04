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

import scala.Tuple2;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.comparison.ComparableOrToStringComparator;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.parquetstore.index.ColumnIndex;
import uk.gov.gchq.gaffer.parquetstore.index.GraphIndex;
import uk.gov.gchq.gaffer.parquetstore.index.GroupIndex;
import uk.gov.gchq.gaffer.parquetstore.index.MinValuesWithPath;
import uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl.rdd.CalculateSplitPointsFromJavaRDD;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreUtils.invokeSplitPointCalculations;

/**
 * Generates the split points from the {@link GraphIndex} and uses the min values per file as the split points
 */
public final class CalculateSplitPointsFromIndex {

    private static final ComparableOrToStringComparator COMPARATOR = new ComparableOrToStringComparator();

    private CalculateSplitPointsFromIndex() {
    }

    public static Map<String, Map<Object, Integer>> apply(final GraphIndex index, final SchemaUtils schemaUtils,
                                                          final ParquetStoreProperties properties,
                                                          final Iterable<? extends Element> data,
                                                          final ExecutorService pool) throws SerialisationException, OperationException {
        final Map<String, Map<Object, Integer>> groupToSplitPoints = calculateSplitPointsFromIndex(index, schemaUtils);
        final List<Callable<Tuple2<String, Map<Object, Integer>>>> tasks = new ArrayList<>();
        for (final String group : schemaUtils.getEntityGroups()) {
            if (!groupToSplitPoints.containsKey(group)) {
                tasks.add(new CalculateSplitPointsFromIterable(properties.getSampleRate(), properties.getAddElementsOutputFilesPerGroup() - 1, data, group, true));
            }
        }
        for (final String group : schemaUtils.getEdgeGroups()) {
            if (!groupToSplitPoints.containsKey(group)) {
                tasks.add(new CalculateSplitPointsFromIterable(properties.getSampleRate(), properties.getAddElementsOutputFilesPerGroup() - 1, data, group, false));
            }
        }
        invokeSplitPointCalculations(pool, tasks, groupToSplitPoints);
        return groupToSplitPoints;
    }

    public static Map<String, Map<Object, Integer>> apply(final GraphIndex index, final SchemaUtils schemaUtils,
                                                          final ParquetStoreProperties properties,
                                                          final JavaRDD<Element> data,
                                                          final ExecutorService pool) throws SerialisationException, OperationException {
        final Map<String, Map<Object, Integer>> groupToSplitPoints = calculateSplitPointsFromIndex(index, schemaUtils);
        final List<Callable<Tuple2<String, Map<Object, Integer>>>> tasks = new ArrayList<>();
        for (final String group : schemaUtils.getEntityGroups()) {
            if (!groupToSplitPoints.containsKey(group)) {
                tasks.add(new CalculateSplitPointsFromJavaRDD(properties.getSampleRate(), properties.getAddElementsOutputFilesPerGroup() - 1, data, group, true));
            }
        }
        for (final String group : schemaUtils.getEdgeGroups()) {
            if (!groupToSplitPoints.containsKey(group)) {
                tasks.add(new CalculateSplitPointsFromJavaRDD(properties.getSampleRate(), properties.getAddElementsOutputFilesPerGroup() - 1, data, group, true));
            }
        }
        invokeSplitPointCalculations(pool, tasks, groupToSplitPoints);
        return groupToSplitPoints;
    }

    private static Map<String, Map<Object, Integer>> calculateSplitPointsFromIndex(final GraphIndex index, final SchemaUtils schemaUtils) throws SerialisationException {
        final Set<String> entityGroups = schemaUtils.getEntityGroups();
        final Map<String, Map<Object, Integer>> groupToSplitPoints = new HashMap<>();
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
            final Map<Object, Integer> splitPoints = new TreeMap<>(COMPARATOR);
            final Iterator<MinValuesWithPath> indexIter = columnIndex.getIterator();
            while (indexIter.hasNext()) {
                final MinValuesWithPath minValuesWithPath = indexIter.next();
                final Object gafferObject = converter.parquetObjectsToGafferObject(col, minValuesWithPath.getMin());
                final int split = Integer.valueOf(minValuesWithPath.getPath().substring(5, 10));
                splitPoints.put(gafferObject, split);
            }
            if (!splitPoints.isEmpty()) {
                groupToSplitPoints.put(group, splitPoints);
            }
        }
        return groupToSplitPoints;
    }
}
