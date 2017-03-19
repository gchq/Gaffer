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
package uk.gov.gchq.gaffer.named.operation.handler;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import uk.gov.gchq.gaffer.named.operation.DeleteNamedOperation;
import uk.gov.gchq.gaffer.named.operation.cache.CacheOperationFailedException;
import uk.gov.gchq.gaffer.named.operation.cache.INamedOperationCache;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

/**
 * Operation Handler for DeleteNamedOperation.
 */
public class DeleteNamedOperationHandler implements OperationHandler<DeleteNamedOperation, Void> {
    private INamedOperationCache cache;

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
            if (cache == null) {
                throw new OperationException("Cache should be initialised in " +
                        "resources/NamedOperationsDeclarations.json and referenced in store.properties");
            }
            cache.deleteNamedOperation(operation.getOperationName(), context.getUser());
        } catch (CacheOperationFailedException e) {
            throw new OperationException(e.getMessage(), e);
        }
        return null;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public INamedOperationCache getCache() {
        return cache;
    }

    public void setCache(final INamedOperationCache cache) {
        this.cache = cache;
    }
}
