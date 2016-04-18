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

package gaffer.arrayliststore.operation.handler;

import gaffer.arrayliststore.ArrayListStore;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.element.Properties;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.operation.OperationException;
import gaffer.operation.simple.UpdateStore;
import gaffer.store.Store;
import gaffer.store.StoreException;
import gaffer.store.StoreProperties;
import gaffer.store.operation.handler.OperationHandler;
import gaffer.store.schema.Schema;
import java.util.Map.Entry;

public class UpdateStoreHandler implements OperationHandler<UpdateStore, Void> {
    @Override
    public Void doOperation(final UpdateStore operation, final Store store) throws OperationException {
        return doOperation(operation, (ArrayListStore) store);
    }

    private Void doOperation(final UpdateStore operation, final ArrayListStore store) throws OperationException {
        migrateElements(operation, store);

        final Schema newSchema = null != operation.getNewSchema() ? operation.getNewSchema() : store.getSchema();
        final StoreProperties storeProperties = null != operation.getNewStoreProperties() ? operation.getNewStoreProperties() : store.getProperties();
        try {
            store.initialise(newSchema, storeProperties);
        } catch (StoreException e) {
            throw new OperationException("Unable to update store schema and properties", e);
        }

        return null;
    }

    private void migrateElements(final UpdateStore operation, final ArrayListStore store) {
        final View view = operation.getView();
        if (hasTransforms(view)) {
            if (!view.getEntityGroups().isEmpty()) {
                for (final Entity entity : store.getEntities()) {
                    if (operation.validateFilter(entity)) {
                        final ViewElementDefinition elementDef = view.getEntity(entity.getGroup());
                        if (null != elementDef && null != elementDef.getTransformer()) {
                            elementDef.getTransformer().transform(entity);
                            removeNullProperties(entity);
                        }
                    }
                }
            }

            if (!view.getEdgeGroups().isEmpty()) {
                for (final Edge edge : store.getEdges()) {
                    if (operation.validateFilter(edge)) {
                        final ViewElementDefinition elementDef = view.getEdge(edge.getGroup());
                        if (null != elementDef && null != elementDef.getTransformer()) {
                            elementDef.getTransformer().transform(edge);
                            removeNullProperties(edge);
                        }
                    }
                }
            }
        }
    }

    private boolean hasTransforms(final View view) {
        for (ViewElementDefinition elementDef : view.getEntities().values()) {
            if (null != elementDef.getTransformer() && !elementDef.getTransformer().getFunctions().isEmpty()) {
                return true;
            }
        }

        for (ViewElementDefinition elementDef : view.getEdges().values()) {
            if (null != elementDef.getTransformer() && !elementDef.getTransformer().getFunctions().isEmpty()) {
                return true;
            }
        }

        return false;
    }

    private void removeNullProperties(final Element element) {
        // remove any null properties
        final Properties properties = element.getProperties();
        for (final Entry<String, Object> entry : properties.entrySet()) {
            if (null == entry.getValue()) {
                properties.remove(entry.getKey());
            }
        }
    }
}

