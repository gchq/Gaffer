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

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedOperationOutputHandler;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.compare.Max;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.compare.MaxHandler;
import java.util.List;

public class FederatedMaxHandler extends FederatedOperationOutputHandler<Max, Element> {
    private final MaxHandler handler;

    public FederatedMaxHandler() {
        this(new MaxHandler());
    }

    public FederatedMaxHandler(final MaxHandler handler) {
        this.handler = (handler == null) ? new MaxHandler() : handler;
    }

    @Override
    protected Element mergeResults(final List<Element> results, final Max operation, final Context context, final Store store) throws OperationException {
        operation.setInput(results);
        return handler.doOperation(operation, context, store);
    }
}
