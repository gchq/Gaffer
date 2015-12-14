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
 * A predicate that is used to avoid processing every {@link Edge} twice (recall that each
 * {@link Edge} goes into Accumulo twice so that it can be found from either end).
 *
 * NB: This object has no state - therefore all instances of this class are equal.
 * However we cannot use the singleton pattern because we need it to be {@link Writable}
 * and therefore have a no-args constructor.
 */
public class ReturnEdgesOnlyOncePredicate implements Predicate<RawGraphElementWithStatistics> {

    private static final long serialVersionUID = -2729257862416856145L;

    public ReturnEdgesOnlyOncePredicate() { }

    @Override
    public boolean accept(RawGraphElementWithStatistics rawGraphElementWithStatistics) throws IOException {
        if (rawGraphElementWithStatistics.isEntity()) {
            return true;
        }
        // If the edge is undirected then return it if the first (type, value) is <= the second
        // (type, value).
        String firstType = rawGraphElementWithStatistics.getFirstType();
        String firstValue = rawGraphElementWithStatistics.getFirstValue();
        String secondType = rawGraphElementWithStatistics.getSecondType();
        String secondValue = rawGraphElementWithStatistics.getSecondValue();
        if (!rawGraphElementWithStatistics.isDirected()) {
            int compareFirstSecondType = firstType.compareTo(secondType);
            if (compareFirstSecondType < 0) {
                return true;
            } else if (compareFirstSecondType == 0) {
                if (firstValue.compareTo(secondValue) <= 0) {
                    return true;
                }
            }
            return false;
        }
        // If the edge is directed then return it if this is the forward version of this edge
        // (the edge A->B goes into Accumulo as A B -> and B A <- , we only return the former -
        // we can tell this by checking whether the first (type, value) equals the source (type, value)).
        if (firstType.equals(rawGraphElementWithStatistics.getSourceType()) && firstValue.equals(rawGraphElementWithStatistics.getSourceValue())) {
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
        return 1;
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
