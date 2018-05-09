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
package uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities;

import scala.Tuple2;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.comparison.ComparableOrToStringComparator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

/**
 * Generates the split points from an {@link Iterable} of {@link Element}s by selecting a sample of the data, sorting that sample and
 * then pulling out the relevant objects to act as the split points
 */
public class CalculateSplitPointsFromIterable implements Callable<Tuple2<String, Map<Object, Integer>>> {

    private static final ComparableOrToStringComparator COMPARATOR = new ComparableOrToStringComparator();
    private final long sampleRate;
    private final int numOfSplits;
    private final Iterable<? extends Element> data;
    private final String group;
    private final boolean isEntity;

    public CalculateSplitPointsFromIterable(final long sampleRate, final int numOfSplits, final Iterable<? extends Element> data, final String group, final boolean isEntity) {
        this.sampleRate = sampleRate;
        this.numOfSplits = numOfSplits;
        this.data = data;
        this.group = group;
        this.isEntity = isEntity;
    }

    public Tuple2<String, Map<Object, Integer>> call() {
        final Iterator<? extends Element> dataIter = data.iterator();
        final ArrayList<Object> sample = new ArrayList<>();
        long counter = sampleRate;
        while (dataIter.hasNext()) {
            final Element element = dataIter.next();
            if (group.equals(element.getGroup())) {
                if (counter == sampleRate) {
                    if (isEntity) {
                        sample.add(element.getIdentifier(IdentifierType.VERTEX));
                    } else {
                        sample.add(element.getIdentifier(IdentifierType.SOURCE));
                    }
                    counter = 1;
                } else {
                    counter++;
                }
            }
        }
        if (sample.isEmpty()) {
            return new Tuple2<>(group, new TreeMap<>(COMPARATOR));
        } else {
            sample.sort(COMPARATOR);
            final int sampleSize = sample.size();
            final int splitRate = (sampleSize / (numOfSplits + 1)) + 1;
            final Map<Object, Integer> splitPoints = new TreeMap<>(COMPARATOR);
            for (int i = 0; i < sampleSize; i += splitRate) {
                splitPoints.put(sample.get(i), i / splitRate);
            }
            if (splitPoints.isEmpty()) {
                splitPoints.put(sample.get(0), 0);
            }
            return new Tuple2<>(group, splitPoints);
        }
    }
}
