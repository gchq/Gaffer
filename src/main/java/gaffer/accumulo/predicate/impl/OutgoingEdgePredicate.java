/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.accumulo.predicate.impl;

import gaffer.accumulo.predicate.RawGraphElementWithStatistics;
import gaffer.graph.Edge;
import gaffer.predicate.Predicate;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A {@link Predicate} of {@link RawGraphElementWithStatistics} that returns true if the supplied
 * {@link RawGraphElementWithStatistics} is an outgoing {@link Edge}, i.e. it goes out of
 * the first type-value in the row.
 *
 * NB: This object has no state - therefore all instances of this class are equal.
 * However we cannot use the singleton pattern because we need it to be {@link Writable}
 * and therefore have a no-args constructor.
 */
public class OutgoingEdgePredicate implements Predicate<RawGraphElementWithStatistics> {

    private static final long serialVersionUID = -5911964647738044143L;

    public OutgoingEdgePredicate() { }

    @Override
    public boolean accept(RawGraphElementWithStatistics rawGraphElementWithStatistics) throws IOException {
        if (rawGraphElementWithStatistics.isEntity()) {
            return true;
        }
        // If the edge is undirected then return it.
        if (!rawGraphElementWithStatistics.isDirected()) {
            return true;
        }
        // If the edge is directed then only return it if it is outgoing from the first (type, value),
        // i.e. if the first type-value equals the source type-value
        String firstType = rawGraphElementWithStatistics.getFirstType();
        String firstValue = rawGraphElementWithStatistics.getFirstValue();
        String sourceType = rawGraphElementWithStatistics.getSourceType();
        String sourceValue = rawGraphElementWithStatistics.getSourceValue();
        if (firstType.equals(sourceType) && firstValue.equals(sourceValue)) {
            return true;
        }
        return false;
    }

    @Override
    public void write(DataOutput out) throws IOException { }

    @Override
    public void readFields(DataInput in) throws IOException { }

    @Override
    public int hashCode() {
        // Return an arbitrary value - see comment above about why we cannot use
        // the singleton pattern.
        return 2;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        return getClass() == obj.getClass();
    }

}
