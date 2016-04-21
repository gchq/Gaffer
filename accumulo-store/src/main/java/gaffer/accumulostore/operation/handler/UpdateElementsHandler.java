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
import gaffer.accumulostore.key.AccumuloKeyPackage;
import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.accumulostore.utils.TableUtils;
import gaffer.commonutil.Pair;
import gaffer.data.element.Element;
import gaffer.data.element.Properties;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.operation.OperationException;
import gaffer.operation.simple.UpdateElements;
import gaffer.store.ElementValidator;
import gaffer.store.Store;
import gaffer.store.StoreException;
import gaffer.store.operation.handler.OperationHandler;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map.Entry;

public class UpdateElementsHandler implements OperationHandler<UpdateElements, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateElementsHandler.class);

    @Override
    public Void doOperation(final UpdateElements operation, final Store store)
            throws OperationException {
        return doOperation(operation, (AccumuloStore) store);
    }

    public Void doOperation(final UpdateElements operation, final AccumuloStore store) throws OperationException {
        try {
            updateElements(operation, store);
        } catch (StoreException e) {
            throw new OperationException("Failed to updated elements", e);
        }

        return null;
    }

    private void updateElements(final UpdateElements operation, final AccumuloStore store) throws OperationException, StoreException {
        final BatchWriter writer = TableUtils.createBatchWriter(store);
        final ElementValidator validator = new ElementValidator(store.getSchema());
        final AccumuloKeyPackage keyPackage = store.getKeyPackage();
        for (final Pair<Element> elementPair : operation.getElementPairs()) {
            deleteElement(keyPackage, writer, elementPair.getFirst());
            addElement(elementPair, operation, validator, keyPackage, writer);
        }

        try {
            writer.close();
        } catch (final MutationsRejectedException e) {
            LOGGER.warn("Accumulo batch writer failed to close", e);
        }
    }

    private void deleteElement(final AccumuloKeyPackage keyPackage, final BatchWriter writer, final Element element) {
        Pair<Key> keys;
        try {
            keys = keyPackage.getKeyConverter().getKeysFromElement(element);
        } catch (final AccumuloElementConversionException e) {
            LOGGER.error("Failed to create an accumulo key from element of type " + element.getGroup()
                    + " when trying to insert elements");
            keys = null;
        }

        if (null != keys) {
            writeDelete(writer, keys.getFirst());
            if (null != keys.getSecond()) {
                writeDelete(writer, keys.getSecond());
            }
        }
    }

    private void addElement(final Pair<Element> elementPair, final UpdateElements operation, final ElementValidator elementValidator, final AccumuloKeyPackage keyPackage, final BatchWriter writer) {
        final Element elementToAdd = elementPair.getSecond();
        if (!operation.isOverrideProperties()) {
            // Add existing properties that are not provided in new element.
            final Properties newProps = elementToAdd.getProperties();
            for (Entry<String, Object> prop : elementPair.getFirst().getProperties().entrySet()) {
                if (!newProps.containsKey(prop.getKey())) {
                    elementToAdd.putProperty(prop.getKey(), prop.getValue());
                }
            }
        }

        if (!operation.isValidate() || elementValidator.validate(elementToAdd)) {
            try {
                final Pair<Key> keys = keyPackage.getKeyConverter().getKeysFromElement(elementToAdd);
                final Value value = keyPackage.getKeyConverter().getValueFromElement(elementToAdd);
                writeAdd(writer, keys.getFirst(), value);
                if (null != keys.getSecond()) {
                    writeAdd(writer, keys.getFirst(), value);

                }
            } catch (final AccumuloElementConversionException e) {
                LOGGER.error("Failed to create an accumulo value from element of type " + elementToAdd.getGroup()
                        + " when trying to insert elements");
            }
        } else if (!operation.isSkipInvalidElements()) {
            throw new IllegalArgumentException("Element is not valid: " + elementToAdd.toString());
        }
    }

    private long getNewTimestamp(final Pair<Key> keys, final Pair<Key> transformedKeys) {
        long newTimestamp = transformedKeys.getFirst().getTimestamp();
        if (newTimestamp <= keys.getFirst().getTimestamp()) {
            if (keys.getFirst().getTimestamp() < Long.MAX_VALUE) {
                newTimestamp = keys.getFirst().getTimestamp() + 1;
            } else {
                LOGGER.error("Cannot increase timestamp as it is already set to MAX_VALUE - the updated element will not be added");
            }
        }
        return newTimestamp;
    }

    private void writeAdd(final BatchWriter writer, final Key key, final Value value) {
        final Mutation mAdd = new Mutation(key.getRow());
        mAdd.put(key.getColumnFamily(), key.getColumnQualifier(),
                new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value);
        try {
            writer.addMutation(mAdd);
        } catch (final MutationsRejectedException e) {
            LOGGER.error("Failed to create an accumulo key mutation");
        }
    }

    private void writeDelete(final BatchWriter writer, final Key key) {
        final Mutation mDelete = new Mutation(key.getRow());
        mDelete.putDelete(key.getColumnFamily(), key.getColumnQualifier(),
                new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp());
        try {
            writer.addMutation(mDelete);
        } catch (final MutationsRejectedException e) {
            LOGGER.error("Failed to create an accumulo key mutation");
        }
    }

    private View createTransformlessView(final View view) {
        // Clone the view
        final View transformlessView = View.fromJson(view.toJson(false));

        // Delete the transforms
        for (ViewElementDefinition elementDef : transformlessView.getEntities().values()) {
            elementDef.setTransformer(null);
        }
        for (ViewElementDefinition elementDef : transformlessView.getEdges().values()) {
            elementDef.setTransformer(null);
        }

        return transformlessView;
    }
}
