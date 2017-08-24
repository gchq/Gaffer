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

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.comparison.ComparableOrToStringComparator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Generates the split points from an {@link Iterable} of {@link Element}s by selecting a sample of the data, sorting that sample and
 * then pulling out the relevant objects to act as the split points
 */
public class CalculateSplitPointsFromIterable {

    private static final ComparableOrToStringComparator COMPARATOR = new ComparableOrToStringComparator();
    private final long sampleRate;
    private final int numOfSplits;

    public CalculateSplitPointsFromIterable(final long sampleRate, final int numOfSplits) {
        this.sampleRate = sampleRate;
        this.numOfSplits = numOfSplits;
    }

    public Map<Integer, Object> calculateSplitsForGroup(final Iterable<? extends Element> data, final String group, final boolean isEntity) {
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
                    counter = 0;
                } else {
                    counter++;
                }
            }
        }
        if (sample.isEmpty()) {
            return new TreeMap<>(COMPARATOR);
        } else {
            sample.sort(COMPARATOR);
            final int x = (sample.size() / (numOfSplits + 1)) + 1;
            final Map<Integer, Object> splitPoints = new TreeMap<>(COMPARATOR);
            if (x == 0) {
                splitPoints.put(0, sample.get(0));
            } else {
                counter = 0;
                for (final Object gafferObject : sample) {
                    if (counter % x == 0) {
                        final int i = (int) (counter / x);
                        final Object split = splitPoints.get(i - 1);
                        if (counter == 0 || split == null || !split.equals(gafferObject)) {
                            splitPoints.put(i, gafferObject);
                        }
                    }
                    counter++;
                }
            }
            return splitPoints;
        }
    }
}
