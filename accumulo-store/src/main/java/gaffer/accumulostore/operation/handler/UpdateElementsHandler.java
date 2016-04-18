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
import gaffer.accumulostore.utils.Pair;
import gaffer.accumulostore.utils.TableUtils;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.graph.Graph;
import gaffer.operation.OperationException;
import gaffer.operation.impl.get.GetElementsSeed;
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

        final AccumuloKeyPackage keyPackage = store.getKeyPackage();
        final Iterable<Element> elements = getElements(operation, store);
        for (final Element element : elements) {
            final Pair<Key> keys = deleteElement(keyPackage, writer, element);
            if (keys != null) {
                final ElementValidator elementValidator = new ElementValidator(store.getSchema());
                addTransformedElement(operation, elementValidator, keyPackage, writer, element, keys);
            }
        }

        try {
            writer.close();
        } catch (final MutationsRejectedException e) {
            LOGGER.warn("Accumulo batch writer failed to close", e);
        }
    }

    private Iterable<Element> getElements(final UpdateElements operation, final AccumuloStore store) throws OperationException {
        final View transformlessView = createTransformlessView(operation.getView());
        final Graph graph = new Graph.Builder()
                .store(store)
                .build();
        return graph.execute(new GetElementsSeed.Builder<>()
                .seeds(operation.getSeeds())
                .view(transformlessView)
                .build());
    }

    private Pair<Key> deleteElement(final AccumuloKeyPackage keyPackage, final BatchWriter writer, final Element element) {
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
        return keys;
    }

    private void addTransformedElement(final UpdateElements operation, final ElementValidator elementValidator, final AccumuloKeyPackage keyPackage, final BatchWriter writer, final Element element, final Pair<Key> keys) {
        if (transformElement(element, operation.getView(), elementValidator)) {
            final Pair<Key> transformedKeys;
            try {
                transformedKeys = keyPackage.getKeyConverter().getKeysFromElement(element);
            } catch (final AccumuloElementConversionException e) {
                LOGGER.error("Failed to create an accumulo key from element of type " + element.getGroup()
                        + " when trying to insert elements");
                return;
            }
            final Value transformedValue;
            try {
                transformedValue = keyPackage.getKeyConverter().getValueFromElement(element);
            } catch (final AccumuloElementConversionException e) {
                LOGGER.error("Failed to create an accumulo transformedValue from element of type " + element.getGroup()
                        + " when trying to insert elements");
                return;
            }

            final long newTimestamp = getNewTimestamp(keys, transformedKeys);
            transformedKeys.getFirst().setTimestamp(newTimestamp);
            writeAdd(writer, transformedKeys.getFirst(), transformedValue);
            if (null != transformedKeys.getSecond()) {
                transformedKeys.getSecond().setTimestamp(newTimestamp);
                writeAdd(writer, transformedKeys.getFirst(), transformedValue);

            }
        }
    }

    private boolean transformElement(final Element element, final View view, final ElementValidator elementValidator) {
        boolean success = false;
        final ViewElementDefinition elementDef = view.getElement(element.getGroup());
        if (null != elementDef && null != elementDef.getTransformer()) {
            elementDef.getTransformer().transform(element);
            if (elementValidator.validate(element)) {
                success = true;
            } else {
                LOGGER.warn("Invalid element: " + element);
            }
        }

        return success;
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
