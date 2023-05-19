/*
 * Copyright 2017-2023 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedOperationCache;
import uk.gov.gchq.gaffer.store.operation.resolver.ScoreResolver;

import java.util.List;

/**
 * A <code>NamedOperationScoreResolver</code> will resolve the custom Operation
 * Score for a provided {@link NamedOperation} by searching for it within the
 * {@link NamedOperationCache}.
 */
public class NamedOperationScoreResolver implements ScoreResolver<NamedOperation> {
    private final NamedOperationCache cache;

    /**
     * @param namedOperationCacheNameSuffix the suffix of NamedOperationCache to score against.
     */
    public NamedOperationScoreResolver(@JsonProperty("namedOperationCacheNameSuffix") final String namedOperationCacheNameSuffix) {
        this(new NamedOperationCache(namedOperationCacheNameSuffix));
        if (Strings.isNullOrEmpty(namedOperationCacheNameSuffix)) {
            LOGGER.error(NamedOperationCache.NAMED_OPERATION_CACHE_WAS_MADE_WITH_NULL_OR_EMPTY_SUFFIX);
        }
    }

    public String getNamedOperationCacheNameSuffix() {
        final String cacheName = cache.getCacheName();
        return (NamedOperationCache.CACHE_SERVICE_NAME_PREFIX.equals(cacheName))
                ? cacheName
                : cacheName.substring(NamedOperationCache.CACHE_SERVICE_NAME_PREFIX.length() + 1);
    }

    public NamedOperationScoreResolver(final NamedOperationCache cache) {
        this.cache = cache;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(NamedOperationScoreResolver.class);

    @Override
    public Integer getScore(final NamedOperation operation) {
        return getScore(operation, null);
    }

    @Override
    public Integer getScore(final NamedOperation operation, final ScoreResolver defaultScoreResolver) {
        Integer namedOpScore = null;
        NamedOperationDetail namedOpDetail = null;
        if (null == operation) {
            return 0;
        }

        try {
            namedOpDetail = cache.getFromCache(operation.getOperationName());
        } catch (final CacheOperationException e) {
            LOGGER.warn("Error accessing cache for Operation '{}': {}", operation.getClass().getName(), e.getMessage());
        }

        if (null != namedOpDetail) {
            namedOpScore = namedOpDetail.getScore();
            if (null == namedOpScore && null != defaultScoreResolver) {
                namedOpScore = defaultScoreResolver.getScore(namedOpDetail.getOperationChain(operation.getParameters()));
            }
        }
        if (null != defaultScoreResolver) {
            if (null == namedOpScore) {
                namedOpScore = 0;
            }
            List parameterOperations = operation.getOperations();
            if (null != parameterOperations) {
                for (final Object objectOperation : parameterOperations) {
                    Operation op = (Operation) objectOperation;
                    Integer parameterOpScore = defaultScoreResolver.getScore(op);
                    namedOpScore += parameterOpScore;
                }
            }
        }

        return namedOpScore;
    }
}
