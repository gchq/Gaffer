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

/**
 * A <code>FetchCachedResult</code> fetches a cached result {@link Iterable} for a
 * provided key. If a key is not provided the default key is 'ALL'.
 * The cache is maintained per single operation chain only. It cannot be used
 * across multiple separate operation requests. So, it must be updated and
 * fetched inside a single operation chain.
 *
 * @see UpdateCache
 * @see FetchCache
 */
public class FetchCachedResult extends AbstractOperation<Void, Iterable<?>> implements CacheOperation {
    private String key;

    /**
     * Constructs an <code>FetchCachedResult</code> with the key set to 'ALL'.
     */
    public FetchCachedResult() {
        this(UpdateCache.ALL);
    }

    /**
     * Constructs an <code>FetchCachedResult</code> with the provided key.
     *
     * @param key the key to use to fetch results from the cache.
     */
    public FetchCachedResult(final String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void setKey(final String key) {
        this.key = key;
    }

    public static class Builder extends AbstractOperation.Builder<FetchCachedResult, Void, Iterable<?>> {
        public Builder() {
            super(new FetchCachedResult());
        }

        public Builder key(final String key) {
            getOp().setKey(key);
            return this;
        }

        @Override
        public Builder option(final String name, final String value) {
            super.option(name, value);
            return this;
        }
    }
}
