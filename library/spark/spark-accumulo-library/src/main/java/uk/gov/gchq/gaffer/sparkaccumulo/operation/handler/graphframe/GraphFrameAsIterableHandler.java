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

package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.graphframe;

import com.google.common.collect.Iterators;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.operation.graphframe.GraphFrameAsIterable;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.Iterator;

public class GraphFrameAsIterableHandler implements OutputOperationHandler<GraphFrameAsIterable, Iterable<? extends Row>> {

    @Override
    public Iterable<? extends Row> doOperation(final GraphFrameAsIterable operation, final Context context, final Store store) throws OperationException {

        if (null == operation.getInput()) {
            throw new OperationException("Input must not be null");
        }

        final GraphFrame graphFrame = operation.getInput();

        final Iterator<Row> vertices = graphFrame.vertices().toLocalIterator();
        final Iterator<Row> edges = graphFrame.edges().toLocalIterator();

        return () -> Iterators.concat(vertices, edges);
    }
}
