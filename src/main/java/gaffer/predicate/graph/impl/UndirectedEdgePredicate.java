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
package gaffer.predicate.graph.impl;

import gaffer.graph.Entity;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.predicate.Predicate;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A {@link Predicate} that returns <code>true</code> if the provided {@link
 * GraphElementWithStatistics} contains an undirected edge. Note that this predicate
 * also returns <code>true</code> if the {@link GraphElementWithStatistics} contains
 * an {@link Entity} - if this is not desired then it should be combined with
 * {@link IsEdgePredicate}.
 *
 * NB: This object has no state - therefore all instances of this class are equal.
 * However we cannot use the singleton pattern because we need it to be {@link Writable}
 * and therefore have a no-args constructor.
 */
public class UndirectedEdgePredicate implements Predicate<GraphElementWithStatistics> {

    private static final long serialVersionUID = 3103343991830306474L;

    public UndirectedEdgePredicate() { }

    @Override
    public boolean accept(GraphElementWithStatistics graphElementWithStatistics) throws IOException {
        /* NB: If this is an Entity, then return true. This is because we may have a view
        that says entities and directed edges are wanted. So we need to return true here.
        If only edges are required then the removal of Entitys will be handled separately
        (either by a predicate or by the ranges that are set).
        */
        if (graphElementWithStatistics.isEntity()) {
            return true;
        }
        return !graphElementWithStatistics.isDirected();
    }

    @Override
    public void write(DataOutput out) throws IOException { }

    @Override
    public void readFields(DataInput in) throws IOException { }

    @Override
    public int hashCode() {
        // Return an arbitrary value - see comment above about why we cannot use
        // the singleton pattern.
        return 4;
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
