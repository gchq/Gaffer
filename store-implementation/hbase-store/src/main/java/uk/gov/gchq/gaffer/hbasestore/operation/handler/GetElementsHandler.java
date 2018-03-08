/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.hbasestore.operation.handler;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.user.User;

public class GetElementsHandler implements OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> {
    @Override
    public CloseableIterable<? extends Element> doOperation(final GetElements operation, final Context context, final Store store) throws OperationException {
        return doOperation(operation, context.getUser(), (HBaseStore) store);
    }

    private CloseableIterable<? extends Element> doOperation(final GetElements operation, final User user, final HBaseStore store) throws OperationException {
        if (null == operation.getInput()) {
            // If null seeds no results are returned
            return new WrappedCloseableIterable<>();
        }

        if (null != operation.getOption("hbasestore.operation.return_matched_id_as_edge_source")) {
            throw new IllegalArgumentException("The hbasestore.operation.return_matched_id_as_edge_source option has been removed. " +
                    "Instead of flipping the Edges around the result Edges will have a matchedVertex field set specifying if the SOURCE or DESTINATION was matched.");
        }
        try {
            return store.createRetriever(operation, user, operation.getInput(), SeedMatching.SeedMatchingType.EQUAL != operation.getSeedMatching());
        } catch (final StoreException e) {
            throw new OperationException("Unable to fetch elements", e);
        }
    }
}
