/*
 * Copyright 2016 Crown Copyright
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

package gaffer.store.operation.handler;

import gaffer.operation.OperationException;
import gaffer.operation.impl.cache.FetchCachedResult;
import gaffer.store.Context;
import gaffer.store.Store;
import java.util.Collections;

/**
 * An <code>FetchCacheHandler</code> handles {@link FetchCachedResult} operations.
 * Simply returns the cache.
 */
public class FetchCachedResultHandler implements OperationHandler<FetchCachedResult, Iterable<?>> {
    @Override
    public Iterable<?> doOperation(final FetchCachedResult fetchCachedResult,
                                   final Context context, final Store store)
            throws OperationException {
        Iterable<?> result = context.getCache().get(fetchCachedResult.getKey());
        if (null == result) {
            result = Collections.emptySet();
        }

        return result;
    }
}
