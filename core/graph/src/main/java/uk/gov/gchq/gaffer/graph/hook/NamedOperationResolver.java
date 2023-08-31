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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static uk.gov.gchq.gaffer.store.operation.handler.util.OperationHandlerUtil.updateOperationInput;

/**
 * A {@link GraphHook} to resolve named operations.
 */
@JsonPropertyOrder(alphabetic = true)
public class NamedOperationResolver implements GraphHook, GetFromCacheHook {
    private static final Logger LOGGER = LoggerFactory.getLogger(NamedOperationResolver.class);
    private final NamedOperationCache cache;
    private int timeout = 1;
    private TimeUnit timeUnit = TimeUnit.MINUTES;

    public NamedOperationResolver(final String suffixNamedOperationCacheName) {
        this(suffixNamedOperationCacheName, 1, TimeUnit.MINUTES);
    }

    @JsonCreator
    public NamedOperationResolver(@JsonProperty("suffixNamedOperationCacheName") final String suffixNamedOperationCacheName,
                                  @JsonProperty("timeout") final int timeout,
                                  @JsonProperty("timeUnit") final TimeUnit timeUnit) {
        this(new NamedOperationCache(suffixNamedOperationCacheName));
        this.timeout = timeout;
        this.timeUnit = timeUnit;
    }

    public NamedOperationResolver(final NamedOperationCache cache) {
        this.cache = cache;
    }

    @JsonGetter("suffixNamedOperationCacheName")
    public String getSuffixCacheName() {
        return cache.getSuffixCacheName();
    }

    public int getTimeout() {
        return timeout;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final Future<?> future = executor.submit(new NamedOperationResolverTask(opChain, context.getUser(), cache));

        final String s = timeout + timeUnit.name();
        try {
            LOGGER.info("Starting ResolverTask with timeout: " + s);
            future.get(timeout, timeUnit);
            LOGGER.info("finished ResolverTask");
        } catch (TimeoutException e) {
            throw new GafferRuntimeException("ResolverTask timed out after: " + s);
        } catch (InterruptedException e) {
            throw new GafferRuntimeException("Future interrupted out");
        } catch (ExecutionException e) {
            throw new GafferRuntimeException("ResolverTask failed due to: " + e.getMessage(), e);
        } finally {
            executor.shutdownNow();
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
        final List<Operation> updatedOperations = new ArrayList<>(operations.getOperations().size());
        for (final Operation operation : operations.getOperations()) {
            if (Thread.interrupted()) {
                return;
            }
            if (operation instanceof NamedOperation) {
                updatedOperations.addAll(resolveNamedOperation((NamedOperation) operation, user, cache));
            } else {
                if (operation instanceof Operations) {
                    resolveNamedOperations(((Operations<?>) operation), user, cache);
                }
                updatedOperations.add(operation);
            }
        }
        operations.updateOperations((List) updatedOperations);
    }

    private static List<Operation> resolveNamedOperation(final NamedOperation namedOp, final User user, final NamedOperationCache cache) {
        final NamedOperationDetail namedOpDetail;
        try {
            namedOpDetail = cache.getNamedOperation(namedOp.getOperationName(), user);
        } catch (final CacheOperationException e) {
            // Unable to find named operation - just return the original named operation
            // Exception messages are lost so Log them
            LOGGER.error(e.getMessage());
            return Collections.singletonList(namedOp);
        }

        final OperationChain<?> namedOperationChain = namedOpDetail.getOperationChain(namedOp.getParameters());
        updateOperationInput(namedOperationChain, namedOp.getInput());

        // Call resolveNamedOperations again to check there are no nested named operations
        resolveNamedOperations(namedOperationChain, user, cache);
        return namedOperationChain.getOperations();
    }
}
