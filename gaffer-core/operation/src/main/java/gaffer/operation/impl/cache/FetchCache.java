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

package gaffer.operation.impl.cache;

import gaffer.operation.AbstractOperation;
import gaffer.operation.cache.CacheOperation;
import java.util.Map;

/**
 * A <code>FetchCache</code> fetches the entire cache {@link Map}.
 * The cache is maintained per single operation chain only. It cannot be used
 * across multiple separate operation requests. So, it must be updated and
 * fetched inside a single operation chain.
 *
 * @see UpdateCache
 * @see FetchCachedResult
 */
public class FetchCache extends AbstractOperation<Void, Map<String, Iterable<?>>> implements CacheOperation {
    public static class Builder extends AbstractOperation.Builder<FetchCache, Void, Map<String, Iterable<?>>> {
        public Builder() {
            super(new FetchCache());
        }

        @Override
        public Builder option(final String name, final String value) {
            super.option(name, value);
            return this;
        }
    }
}
