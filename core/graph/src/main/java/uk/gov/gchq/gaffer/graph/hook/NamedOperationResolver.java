/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.graph.hook;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedOperationCache;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static uk.gov.gchq.gaffer.store.operation.handler.util.OperationHandlerUtil.updateOperationInput;

/**
 * A {@link GraphHook} to resolve named operations.
 */
@JsonPropertyOrder(alphabetic = true)
public class NamedOperationResolver implements GetFromCacheHook {
    private static final Logger LOGGER = LoggerFactory.getLogger(NamedOperationResolver.class);
    public static final int TIMEOUT_DEFAULT = 1;
    public static final TimeUnit TIME_UNIT_DEFAULT = TimeUnit.MINUTES;
    private final NamedOperationCache cache;
    private final int timeout;
    private final TimeUnit timeUnit;

    public NamedOperationResolver(final String suffixNamedOperationCacheName) {
        this(suffixNamedOperationCacheName, TIMEOUT_DEFAULT, TIME_UNIT_DEFAULT);
    }

    @JsonCreator
    public NamedOperationResolver(@JsonProperty("suffixNamedOperationCacheName") final String suffixNamedOperationCacheName,
                                  @JsonProperty("timeout") final int timeout,
                                  @JsonProperty("timeUnit") final TimeUnit timeUnit) {
        this(new NamedOperationCache(suffixNamedOperationCacheName), timeout, timeUnit);

    }

    public NamedOperationResolver(final NamedOperationCache cache) {
        this(cache, TIMEOUT_DEFAULT, TIME_UNIT_DEFAULT);
    }

    public NamedOperationResolver(final NamedOperationCache cache,
                                  final int timeout,
                                  final TimeUnit timeUnit) {
        this.cache = cache;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
    }

    @JsonGetter("suffixNamedOperationCacheName")
    public String getSuffixCacheName() {
        return cache.getSuffixCacheName();
    }

    @JsonGetter("timeout")
    public int getTimeout() {
        return timeout;
    }

    @JsonGetter("timeUnit")
    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        CompletableFuture<Void> resolver = CompletableFuture.runAsync(() ->
            NamedOperationResolver.resolveNamedOperations(opChain, context.getUser(), cache));

        final String time = timeout + timeUnit.name();
        try {
            LOGGER.info("Starting ResolverTask with timeout: " + time);
            resolver.get(timeout, timeUnit);
            LOGGER.info("finished ResolverTask");
        } catch (final TimeoutException e) {
            throw new GafferRuntimeException("ResolverTask timed out after: " + time, e);
        } catch (final InterruptedException e) {
            throw new GafferRuntimeException("Future interrupted out", e);
        } catch (final ExecutionException e) {
            throw new GafferRuntimeException("ResolverTask failed due to: " + e.getMessage(), e);
        }
    }

    @Override
    public <T> T postExecute(final T result, final OperationChain<?> opChain, final Context context) {
        return result;
    }

    @Override
    public <T> T onFailure(final T result, final OperationChain<?> opChain, final Context context, final Exception e) {
        return result;
    }

    public static void resolveNamedOperations(final Operations<?> operations, final User user, final NamedOperationCache cache) {
        final List<Operation> updatedOperations = new ArrayList<>();
        operations.getOperations().forEach(operation -> {
            if (operation instanceof NamedOperation) {
                updatedOperations.addAll(resolveNamedOperation((NamedOperation<?, ?>) operation, user, cache));
            } else {
                if (operation instanceof Operations) {
                    resolveNamedOperations(((Operations<?>) operation), user, cache);
                }
                updatedOperations.add(operation);
            }
        });
        operations.updateOperations((List) updatedOperations);
    }

    private static List<Operation> resolveNamedOperation(final NamedOperation<?, ?> namedOp, final User user, final NamedOperationCache cache) {
        final NamedOperationDetail namedOpDetail;
        try {
            namedOpDetail = cache.getNamedOperation(namedOp.getOperationName(), user);
        } catch (final CacheOperationException e) {
            // An Exception with the cache has occurred e.g. it was unable to find named operation
            // and then simply returned the original operation chain with the unresolved NamedOperation.

            // The exception from cache would otherwise be lost, so capture it here and print to LOGS.
            LOGGER.error("Exception resolving NamedOperation within the cache:{}", e.getMessage());
            return Collections.singletonList(namedOp);
        }

        final OperationChain<?> namedOperationChain = namedOpDetail.getOperationChain(namedOp.getParameters());
        updateOperationInput(namedOperationChain, namedOp.getInput());

        // Call resolveNamedOperations again to check there are no nested named operations
        resolveNamedOperations(namedOperationChain, user, cache);
        return namedOperationChain.getOperations();
    }
}
