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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedOperationOutputHandler;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.compare.Sort;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.compare.SortHandler;
import java.util.List;

public class FederatedSortHandler extends FederatedOperationOutputHandler<Sort, Iterable<? extends Element>> {
    private final SortHandler handler;

    public FederatedSortHandler() {
        this(new SortHandler());
    }

    public FederatedSortHandler(final SortHandler handler) {
        this.handler = (handler == null) ? new SortHandler() : handler;
    }

    @Override
    protected Iterable<? extends Element> mergeResults(final List<Iterable<? extends Element>> results, final Sort operation, final Context context, final Store store) throws OperationException {
        operation.setInput(new ChainedIterable<>(results.toArray(new Iterable[results.size()])));
        return handler.doOperation(operation, context, store);
    }
}
