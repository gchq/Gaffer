/*
 * Copyright 2016-2022 Crown Copyright
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

import org.apache.commons.collections4.CollectionUtils;

import uk.gov.gchq.gaffer.named.operation.GetAllNamedOperations;
import uk.gov.gchq.gaffer.named.operation.NamedOperationDetail;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.serialisation.util.JsonSerialisationUtil;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedOperationCache;
import uk.gov.gchq.koryphe.util.IterableUtil;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Operation Handler for GetAllNamedOperations
 */
public class GetAllNamedOperationsHandler
        implements OutputOperationHandler<GetAllNamedOperations, Iterable<NamedOperationDetail>> {
    private final NamedOperationCache cache;

    public GetAllNamedOperationsHandler() {
        this(new NamedOperationCache());
    }

    public GetAllNamedOperationsHandler(final NamedOperationCache cache) {
        this.cache = cache;
    }

    /**
     * Retrieves all the Named Operations that a user is allowed to see. As the expected behaviour is to bring back a
     * summary of each operation, the simple flag is set to true. This means all the details regarding access roles and
     * operation chain details are not included in the output.
     *
     * @param operation the {@link uk.gov.gchq.gaffer.operation.Operation} to be executed
     * @param context   the operation chain context, containing the user who executed the operation
     * @param store     the {@link Store} the operation should be run on
     * @return an iterable of NamedOperations
     * @throws OperationException thrown if the cache has not been initialized in the operation declarations file
     */
    @Override
    public Iterable<NamedOperationDetail> doOperation(final GetAllNamedOperations operation, final Context context, final Store store)
            throws OperationException {
        final Iterable<NamedOperationDetail> ops = cache.getAllNamedOperations(context.getUser(), store.getProperties().getAdminAuth());
        return IterableUtil.map(ops, new AddInputType());
    }

    private static class AddInputType implements Function<NamedOperationDetail, NamedOperationDetail> {
        @Override
        public NamedOperationDetail apply(final NamedOperationDetail namedOp) {
            if (Objects.nonNull(namedOp) && Objects.isNull(namedOp.getInputType())) {
                try {
                    final List<Operation> opList = namedOp.getOperationChainWithDefaultParams().getOperations();
                    if (CollectionUtils.isNotEmpty(opList)) {
                        final Operation firstOp = opList.get(0);
                        if (firstOp instanceof Input) {
                            namedOp.setInputType(JsonSerialisationUtil.getSerialisedFieldClasses(firstOp.getClass().getName())
                                    .get("input"));
                        }
                    }
                } catch (final Exception e) {
                    // ignore - just don't add the input type
                }
            }
            return namedOp;
        }
    }
}
