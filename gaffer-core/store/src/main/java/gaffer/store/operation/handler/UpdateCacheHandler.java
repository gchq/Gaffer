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

import com.google.common.collect.Iterables;
import gaffer.operation.OperationException;
import gaffer.operation.impl.cache.UpdateCache;
import gaffer.store.Context;
import gaffer.store.Store;
import java.util.Collections;
import java.util.LinkedHashSet;

/**
 * An <code>UpdateCacheHandler</code> handles {@link UpdateCache} operations.
 */
public class UpdateCacheHandler implements OperationHandler<UpdateCache, Iterable<Object>> {
    @Override
    public Iterable<Object> doOperation(final UpdateCache updateCache,
                                        final Context context, final Store store)
            throws OperationException {

        LinkedHashSet<?> cacheSet = (LinkedHashSet<?>) context.getCache().get(updateCache.getKey());
        if (null == cacheSet) {
            cacheSet = new LinkedHashSet<>();
            context.getCache().put(updateCache.getKey(), cacheSet);
        }
        Iterables.addAll(cacheSet, (Iterable) updateCache.getInput());

        return Collections.unmodifiableCollection(cacheSet);
    }
}
