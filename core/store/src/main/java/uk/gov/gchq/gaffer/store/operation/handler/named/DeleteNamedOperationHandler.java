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

package uk.gov.gchq.gaffer.store.operation.handler.named;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.named.operation.DeleteNamedOperation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedOperationCache;

/**
 * Operation Handler for DeleteNamedOperation.
 */
public class DeleteNamedOperationHandler implements OperationHandler<DeleteNamedOperation> {
    private final NamedOperationCache cache;

    @JsonCreator
    public DeleteNamedOperationHandler(@JsonProperty("suffixNamedOperationCacheName") final String suffixNamedOperationCacheName) {
        this(new NamedOperationCache(suffixNamedOperationCacheName));
    }

    public DeleteNamedOperationHandler(final NamedOperationCache cache) {
        this.cache = cache;
    }

    @JsonGetter("suffixNamedOperationCacheName")
    public String getSuffixCacheName() {
        return cache.getSuffixCacheName();
    }

    /**
     * Deletes a NamedOperation from the cache specified in the Operations Declarations file (assuming the user has
     * write privileges on the specified NamedOperation). The user needs only to provide the name of the operation they
     * want to delete.
     *
     * @param operation the {@link uk.gov.gchq.gaffer.operation.Operation} to be executed
     * @param context   the operation chain context, containing the user who executed the operation
     * @param store     the {@link Store} the operation should be run on
     * @return null (as output of this operation is void)
     * @throws OperationException thrown if the user doesn't have permission to delete the NamedOperation
     */
    @Override
    public Void doOperation(final DeleteNamedOperation operation, final Context context, final Store store) throws OperationException {
        try {
            cache.deleteNamedOperation(operation.getOperationName(), context.getUser(), store.getProperties().getAdminAuth());
        } catch (final CacheOperationException e) {
            throw new OperationException(e.getMessage(), e);
        }
        return null;
    }
}
