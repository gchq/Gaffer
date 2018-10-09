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

package uk.gov.gchq.gaffer.parquetstore.operation.handler;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities.ParquetElementRetriever;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.user.User;

import java.util.Iterator;

/**
 * An {@link uk.gov.gchq.gaffer.store.operation.handler.OperationHandler} for the {@link GetElements} operation on the
 * {@link ParquetStore}.
 */
public class GetElementsHandler implements OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> {

    @Override
    public CloseableIterable<? extends Element> doOperation(final GetElements operation,
                                                            final Context context,
                                                            final Store store) throws OperationException {
        final CloseableIterable<? extends Element> result;
        final Iterable<? extends ElementId> input = operation.getInput();
        if (null == input) {
            throw new OperationException("Operation input is undefined - please specify an input.");
        } else {
            final Iterator<? extends ElementId> inputIter = input.iterator();
            if (inputIter.hasNext()) {
                result = doOperation(operation, (ParquetStore) store, context.getUser());
            } else {
                result = new EmptyClosableIterable<>();
            }
            if (inputIter instanceof CloseableIterator) {
                ((CloseableIterator) inputIter).close();
            }
        }
        if (input instanceof CloseableIterable) {
            ((CloseableIterable) input).close();
        }
        return result;
    }

    private CloseableIterable<Element> doOperation(final GetElements operation,
                                                   final ParquetStore store,
                                                   final User user) throws OperationException {
        try {
            return new ParquetElementRetriever(operation.getView(),
                    store,
                    operation.getDirectedType(),
                    operation.getIncludeIncomingOutGoing(),
                    operation.getSeedMatching(),
                    operation.getInput(),
                    user);
        } catch (final StoreException e) {
            throw new OperationException("Failed to getGroup elements", e);
        }
    }
}

