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

package gaffer.accumulostore.operation.handler;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.utils.AddUpdateTableIterator;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.operation.OperationException;
import gaffer.operation.simple.UpdateStore;
import gaffer.store.Store;
import gaffer.store.StoreException;
import gaffer.store.StoreProperties;
import gaffer.store.operation.handler.OperationHandler;
import gaffer.store.schema.Schema;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateStoreHandler implements OperationHandler<UpdateStore, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateStoreHandler.class);

    @Override
    public Void doOperation(final UpdateStore operation, final Store store)
            throws OperationException {
        return doOperation(operation, (AccumuloStore) store);
    }

    public Void doOperation(final UpdateStore operation, final AccumuloStore store) throws OperationException {
        takeTableOffline(store);
        removeIterators(store);
        updateElements(operation, store);
        updateStore(operation, store);
        addIterators(store);
        bringTableOnline(store);
        return null;
    }

    private void updateElements(final UpdateStore operation, final AccumuloStore store) throws OperationException {
        // TODO: enabled updating elements
        if (hasTransforms(operation)) {
            LOGGER.error("Updating all elements is not currently supported.");
        }
    }

    private void takeTableOffline(final AccumuloStore store) throws OperationException {
        try {
            store.getConnection().tableOperations().offline(store.getProperties().getTable(), true);
            LOGGER.info("Table successfully taken offline");
        } catch (StoreException | AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
            throw new OperationException("Failed to take table offline", e);
        }
    }

    private void bringTableOnline(final AccumuloStore store) throws OperationException {
        try {
            store.getConnection().tableOperations().online(store.getProperties().getTable(), true);
            LOGGER.info("Table successfully brought back online");
        } catch (StoreException | AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
            throw new OperationException("Failed to take table offline", e);
        }
    }

    private void removeIterators(final AccumuloStore store) throws OperationException {
        try {
            AddUpdateTableIterator.removeIterators(store);
        } catch (StoreException e) {
            throw new OperationException("Failed to remove iterators. If the iterators are not removed prior to updating the schemas elements may be removed incorrectly by the validation iterator.", e);
        }
    }

    private void addIterators(final AccumuloStore store) throws OperationException {
        try {
            AddUpdateTableIterator.addIterators(store);
        } catch (StoreException e) {
            throw new OperationException("Failed to add iterators", e);
        }
    }

    private void updateStore(final UpdateStore operation, final AccumuloStore store) throws OperationException {
        final Schema newSchema = null != operation.getNewSchema()
                ? operation.getNewSchema() : store.getSchema();
        final StoreProperties newStoreProperties = null != operation.getNewStoreProperties()
                ? operation.getNewStoreProperties() : store.getProperties();

        try {
            store.initialise(newSchema, newStoreProperties);
        } catch (StoreException e) {
            throw new OperationException("Unable to reinitialise store with new schema and properties", e);
        }
    }

    private boolean hasTransforms(final UpdateStore operation) {
        boolean hasTransforms = false;
        for (ViewElementDefinition elementDef : operation.getView().getEntities().values()) {
            if (null != elementDef.getTransformer()) {
                hasTransforms = true;
                break;
            }
        }

        if (!hasTransforms) {
            for (ViewElementDefinition elementDef : operation.getView().getEdges().values()) {
                if (null != elementDef.getTransformer()) {
                    hasTransforms = true;
                    break;
                }
            }
        }

        return hasTransforms;
    }
}
