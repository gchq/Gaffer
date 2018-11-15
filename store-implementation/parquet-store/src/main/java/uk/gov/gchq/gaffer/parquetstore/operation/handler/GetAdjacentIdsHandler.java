/*
 * Copyright 2017. Crown Copyright
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
package uk.gov.gchq.gaffer.parquetstore.operation.handler;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

/**
 * An {@link uk.gov.gchq.gaffer.store.operation.handler.OperationHandler} for the {@link GetAdjacentIds} operation on
 * the {@link uk.gov.gchq.gaffer.parquetstore.ParquetStore}.
 */
public class GetAdjacentIdsHandler
        implements OutputOperationHandler<GetAdjacentIds, CloseableIterable<? extends EntityId>> {

    @Override
    public CloseableIterable<EntitySeed> doOperation(final GetAdjacentIds operation,
                                                     final Context context,
                                                     final Store store) throws OperationException {
        throw new UnsupportedOperationException("The ParquetStore does not implement " + operation.getClass().getSimpleName() + " yet.");
    }
}
