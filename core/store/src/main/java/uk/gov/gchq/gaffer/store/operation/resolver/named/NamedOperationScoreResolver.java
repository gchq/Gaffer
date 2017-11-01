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
package uk.gov.gchq.gaffer.store.operation.resolver.named;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedOperationCache;
import uk.gov.gchq.gaffer.store.operation.resolver.ScoreResolver;

/**
 * A <code>NamedOperationScoreResolver</code> will resolve the custom Operation
 * Score for a provided {@link NamedOperation} by searching for it within the
 * {@link NamedOperationCache}.
 */
public class NamedOperationScoreResolver implements ScoreResolver<NamedOperation> {
    private final NamedOperationCache cache;

    public NamedOperationScoreResolver() {
        this(new NamedOperationCache());
    }

    public NamedOperationScoreResolver(final NamedOperationCache cache) {
        this.cache = cache;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(NamedOperationScoreResolver.class);

    @Override
    public Integer getScore(final NamedOperation operation) {
        Integer namedOpScore = null;
        if (null != operation) {
            try {
                namedOpScore = cache.getFromCache(operation.getOperationName()).getScore();
            } catch (final CacheOperationFailedException e) {
                LOGGER.warn("Error accessing cache for Operation '{}': " + e.getMessage(), operation.getClass().getName());
            }
        }

        return namedOpScore;
    }
}
