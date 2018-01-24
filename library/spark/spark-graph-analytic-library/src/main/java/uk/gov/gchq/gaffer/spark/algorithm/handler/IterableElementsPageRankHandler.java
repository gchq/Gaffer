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

package uk.gov.gchq.gaffer.spark.algorithm.handler;

import org.graphframes.GraphFrame;

import uk.gov.gchq.gaffer.algorithm.PageRank;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

/**
 * The {@code IterableElementsPageRankHandler} operation handler handles {@link uk.gov.gchq.gaffer.spark.algorithm.IterableElementsPageRank}
 * operations.
 * <p>
 * The options are retrieved from the operation object and the operation is delegated
 * to the relevant library.
 */
public class IterableElementsPageRankHandler implements OutputOperationHandler<PageRank<Iterable<? extends Element>>, Iterable<? extends Element>> {

    @Override
    public Iterable<? extends Element> doOperation(final PageRank<Iterable<? extends Element>> operation, final Context context, final Store store) throws OperationException {

        if (null == operation.getInput()) {
            throw new OperationException("Input must not be null.");
        }

        if (!(operation.getInput() instanceof GraphFrame)) {
            throw new OperationException("Incorrect input type. Must be an iterable of elements.");
        }

        throw new UnsupportedOperationException("Determining PageRank from an iterable of elements is not currently supported.");
    }
}
