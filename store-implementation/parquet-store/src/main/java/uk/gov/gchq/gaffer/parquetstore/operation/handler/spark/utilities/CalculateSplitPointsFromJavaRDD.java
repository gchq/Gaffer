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
package uk.gov.gchq.gaffer.parquetstore.operation.handler.spark.utilities;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.comparison.ComparableOrToStringComparator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

/**
 * Generates the split points from an {@link JavaRDD} of {@link Element}s by selecting a sample of the data, sorting that sample and
 * then pulling out the relevant objects to act as the split points
 */
public class CalculateSplitPointsFromJavaRDD implements Callable<Tuple2<String, Map<Object, Integer>>>, Serializable {

    private static final ComparableOrToStringComparator COMPARATOR = new ComparableOrToStringComparator();
    private final long sampleRate;
    private final int numOfSplits;
    private final JavaRDD<Element> data;
    private final String group;
    private final boolean isEntity;

    public CalculateSplitPointsFromJavaRDD(final long sampleRate, final int numOfSplits, final JavaRDD<Element> data,
                                           final String group, final boolean isEntity) {
        this.sampleRate = sampleRate;
        this.numOfSplits = numOfSplits;
        this.data = data;
        this.group = group;
        this.isEntity = isEntity;
    }

    public Tuple2<String, Map<Object, Integer>> call() {
        final JavaRDD<Element> groupFilteredData = data.filter(element -> group.equals(element.getGroup()));
        if (isEntity) {
            return new Tuple2<>(group, calculateSplitsForColumn(groupFilteredData, IdentifierType.VERTEX));
        } else {
            return new Tuple2<>(group, calculateSplitsForColumn(groupFilteredData, IdentifierType.SOURCE));
        }
    }

    private Map<Object, Integer> calculateSplitsForColumn(final JavaRDD<Element> data, final IdentifierType colName) {
        final List<Object> splits = data.sample(false, 1.0 / sampleRate)
                .map(element -> element.getIdentifier(colName))
                .sortBy(obj -> obj, true, numOfSplits)
                .mapPartitions(objectIterator -> {
                    final List<Object> list = new ArrayList<>(1);
                    if (objectIterator.hasNext()) {
                        list.add(objectIterator.next());
                    }
                    return list.iterator();
                })
                .collect();
        final Map<Object, Integer> splitPoints = new TreeMap<>(COMPARATOR);
        int i = 0;
        for (final Object split : splits) {
            if (null != split) {
                splitPoints.put(split, i);
            }
            i++;
        }
        return splitPoints;
    }

}
