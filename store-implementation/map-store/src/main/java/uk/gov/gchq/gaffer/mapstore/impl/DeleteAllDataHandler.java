/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.mapstore.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.DeleteAllData;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

public class DeleteAllDataHandler implements OperationHandler<DeleteAllData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteAllDataHandler.class);

    @Override
    public Object doOperation(final DeleteAllData operation, final Context context, final Store store) throws OperationException {
        LOGGER.info("Removing all data from graph: {}", store.getGraphId());
        removeAllData((MapStore) store);
        return null;
    }

    private void removeAllData(final MapStore mapStore) {
        final MapImpl mapImpl = mapStore.getMapImpl();
        mapImpl.clear();
    }
}
