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

package uk.gov.gchq.gaffer.spark.function;

import com.google.common.collect.Iterators;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;

import java.util.Iterator;
import java.util.function.Function;

/**
 * A {@link Function} to convert a {@link GraphFrame} into an {@link Iterable} of
 * {@link Row}s.
 */
public class GraphFrameToIterableRow implements Function<GraphFrame, Iterable<? extends Row>> {

    @Override
    public Iterable<Row> apply(final GraphFrame graphFrame) {
        final Iterator<Row> vertices = graphFrame.vertices().toLocalIterator();
        final Iterator<Row> edges = graphFrame.edges().toLocalIterator();

        return () -> Iterators.concat(vertices, edges);
    }
}
