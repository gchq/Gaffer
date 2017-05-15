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

import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.named.operation.cache.CacheOperationFailedException;
import uk.gov.gchq.gaffer.named.operation.cache.NamedOperationCache;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.user.User;
import java.util.ArrayList;
import java.util.List;

/**
 * Operation handler for AddNamedOperation which adds a Named Operation to the cache.
 */
public class AddNamedOperationHandler implements OperationHandler<AddNamedOperation> {
    private NamedOperationCache cache = new NamedOperationCache();

    /**
     * Adds a NamedOperation to a cache which must be specified in the operation declarations file. An
     * NamedOperationDetail is built using the fields on the AddNamedOperation. The operation name and operation chain
     * fields must be set and cannot be left empty, or the build() method will fail and a runtime exception will be
     * thrown. The handler then adds/overwrites the NamedOperation according toa an overwrite flag.
     *
     * @param operation the {@link uk.gov.gchq.gaffer.operation.Operation} to be executed
     * @param context   the operation chain context, containing the user who executed the operation
     * @param store     the {@link Store} the operation should be run on
     * @return null (since the output is void)
     * @throws OperationException if the operation on the cache fails
     */
    @Override
    public Void doOperation(final AddNamedOperation operation, final Context context, final Store store) throws OperationException {
        try {
            if (cache == null) {
                throw new OperationException("Cache must not be null");
            }
            validate(context.getUser(), operation.getOperationName(), operation.getOperationChain(), cache);
            NamedOperationDetail namedOperationDetail = new NamedOperationDetail.Builder()
                    .operationChain(operation.getOperationChain())
                    .operationName(operation.getOperationName())
                    .creatorId(context.getUser().getUserId())
                    .readers(operation.getReadAccessRoles())
                    .writers(operation.getWriteAccessRoles())
                    .description(operation.getDescription())
                    .build();

            cache.addNamedOperation(namedOperationDetail, operation.isOverwriteFlag(), context.getUser());
        } catch (final CacheOperationFailedException e) {
            throw new OperationException(e.getMessage(), e);
        }
        return null;
    }

    public NamedOperationCache getCache() {
        return cache;
    }

    public void setCache(final NamedOperationCache cache) {
        this.cache = cache;
    }

    private void validate(final User user, final String parent, final OperationChain<?> operationChain, final NamedOperationCache cache) throws CacheOperationFailedException, OperationException {
        ArrayList<String> parentOperations = new ArrayList<>();
        parentOperations.add(parent);

        validate(user, parentOperations, operationChain, cache);
    }

    private void validate(final User user, final List<String> parentOperations, final OperationChain<?> operationChain, final NamedOperationCache cache) throws CacheOperationFailedException, OperationException {
        for (final Operation op : operationChain.getOperations()) {
            if (op instanceof NamedOperation) {
                if (parentOperations.contains(((NamedOperation) op).getOperationName())) {
                    throw new OperationException("The Operation Chain must not be recursive");
                }
                NamedOperationDetail operation = cache.getNamedOperation(((NamedOperation) op).getOperationName(), user);
                parentOperations.add(((NamedOperation) op).getOperationName());
                validate(user, parentOperations, operation.getOperationChain(), cache);
                parentOperations.remove(((NamedOperation) op).getOperationName());
            }
        }
    }
}
