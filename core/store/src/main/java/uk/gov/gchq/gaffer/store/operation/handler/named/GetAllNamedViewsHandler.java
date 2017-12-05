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

package uk.gov.gchq.gaffer.store.operation.handler.named;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView;
import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;
import uk.gov.gchq.gaffer.named.view.GetAllNamedViews;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedViewCache;

public class GetAllNamedViewsHandler implements OutputOperationHandler<GetAllNamedViews, CloseableIterable<NamedView>> {
    private final NamedViewCache cache;

    public GetAllNamedViewsHandler() {
        this(new NamedViewCache());
    }

    public GetAllNamedViewsHandler(final NamedViewCache cache) {
        this.cache = cache;
    }

    @Override
    public CloseableIterable<NamedView> doOperation(final GetAllNamedViews operation, final Context context, final Store store) throws OperationException {
        try {
            return cache.getAllNamedViews();
        } catch (final CacheOperationFailedException e) {
            throw new OperationException(e.getMessage(), e);
        }
    }
}
