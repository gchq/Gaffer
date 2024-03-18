/*
 * Copyright 2016-2024 Crown Copyright
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
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedOperationCache;
import uk.gov.gchq.gaffer.store.operation.handler.util.OperationHandlerUtil;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A {@link GraphHook} to resolve named operations.
 */
@JsonPropertyOrder(alphabetic = true)
public class NamedOperationResolver implements GetFromCacheHook {
    private static final Logger LOGGER = LoggerFactory.getLogger(NamedOperationResolver.class);
    /**
     * Default depth the resolver will go when checking for nested named operations
     */
    public static final int DEPTH_LIMIT_DEFAULT = 3;

    private final int depthLimit;
    private final NamedOperationCache cache;

    @JsonCreator
    public NamedOperationResolver(
            @JsonProperty("suffixNamedOperationCacheName") final String suffixNamedOperationCacheName,
            @JsonProperty("depthLimit") final int depthLimit) {
        this(new NamedOperationCache(suffixNamedOperationCacheName), depthLimit);
    }

    public NamedOperationResolver(final String suffixNamedOperationCacheName) {
        this(suffixNamedOperationCacheName, DEPTH_LIMIT_DEFAULT);
    }

    public NamedOperationResolver(final NamedOperationCache cache) {
        this(cache, DEPTH_LIMIT_DEFAULT);
    }

    public NamedOperationResolver(final NamedOperationCache cache, final int depthLimit) {
        this.cache = cache;
        this.depthLimit = depthLimit;
    }

    @JsonGetter("suffixNamedOperationCacheName")
    public String getSuffixCacheName() {
        return cache.getSuffixCacheName();
    }

    @JsonGetter("depthLimit")
    public int getDepthLimit() {
        return depthLimit;
    }

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        opChain.updateOperations(resolveNamedOperations(opChain, context.getUser(), 0));
    }

    @Override
    public <T> T postExecute(final T result, final OperationChain<?> opChain, final Context context) {
        return result;
    }

    @Override
    public <T> T onFailure(final T result, final OperationChain<?> opChain, final Context context, final Exception e) {
        return result;
    }

    /**
     * Resolves any named operations from the cache. What is meant
     * by 'resolved' is turning the named operations into their respective
     * OperationChains. Ensures any supplied {@link NamedOperation}s actually
     * exist in the cache and contain their correct {@link Operation}s.
     * Will also run recursively to a given depth limit to ensure any nested
     * {@link NamedOperation}s are also resolved from the cache.
     *
     * @param operation {@link NamedOperation} or {@link Operations} list to act on.
     * @param user      User for the cache access.
     * @param depth     Current recursive depth, will use limit set in class to
     *                  continue or not.
     * @return A list of resolved operations essentially flattened into plan
     *         Operations and OperationChains.
     */
    private Collection<Operation> resolveNamedOperations(final Operation operation, final User user, final int depth) {
        final Collection<Operation> updatedOperations = new ArrayList<>();
        LOGGER.error("Current resolver depth is: {}", depth);

        // If a named operation resolve the operations within it
        if (operation instanceof NamedOperation) {
            NamedOperation<?, ?> namedOperation = (NamedOperation<?, ?>) operation;
            LOGGER.error("Resolving named operation called: {}", namedOperation.getOperationName());
            try {
                // Get the chain for the named operation from the cache
                final OperationChain<?> namedOperationChain = cache
                        .getNamedOperation(namedOperation.getOperationName(), user)
                        .getOperationChain(namedOperation.getParameters());
                // Update the operation inputs and add operation chain to the updated list
                OperationHandlerUtil.updateOperationInput(namedOperationChain, namedOperation.getInput());

                // Run again to get resolve any nested operations in the chain before adding
                namedOperationChain.updateOperations(resolveNamedOperations(namedOperationChain, user, depth + 1));
                updatedOperations.add(namedOperationChain);

            } catch (final CacheOperationException e) {
                LOGGER.error("Exception resolving NamedOperation within the cache: {}", e.getMessage());
            }
        // If given a list of operations then resolve them
        } else if (operation instanceof Operations) {
            LOGGER.error("Resolving Operation List: {}", ((Operations<?>) operation).getOperations());
            for (final Operation op : ((Operations<?>) operation).getOperations()) {
                // If just a plain operation then nothing to resolve
                if (!(op instanceof NamedOperation) && !(op instanceof Operations)) {
                    updatedOperations.add(op);
                    continue;
                }

                // Resolve if haven't hit the depth limit for named operations yet
                // Note only need to check the depth here as when checking for a nested named operation it will always be
                // passed as an operation chain first
                if (depth < depthLimit) {
                    updatedOperations.addAll(resolveNamedOperations(op, user, depth));
                } else {
                    LOGGER.warn("Nested depth limit hit resolving NamedOperations, operation maybe unresolved {}", op);
                    updatedOperations.add(op);
                }

            }
        }
        return updatedOperations;
    }
}
