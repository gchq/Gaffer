/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.job;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.GetGafferResultCacheExport;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobResults;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

public class GetJobResultsHandler implements OperationHandler<GetJobResults, CloseableIterable<?>> {
    @Override
    public CloseableIterable<?> doOperation(final GetJobResults operation, final Context context, final Store store) throws OperationException {
        if (!store.isSupported(GetGafferResultCacheExport.class)) {
            throw new OperationException("Getting job results is not supported as the " + GetGafferResultCacheExport.class.getSimpleName() + " operation has not been configured for this Gaffer graph.");
        }

        // Delegates the operation to the GetGafferResultCacheExport operation handler.
        return store._execute(new OperationChain<>(new GetGafferResultCacheExport.Builder()
                .jobId(operation.getJobId())
                .key(operation.getKey())
                .build()), context);
    }
}
