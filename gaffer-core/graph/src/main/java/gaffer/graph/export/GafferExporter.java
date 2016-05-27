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

package gaffer.graph.export;

import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.commonutil.iterable.LimitedClosableIterable;
import gaffer.data.element.Element;
import gaffer.export.ElementExporter;
import gaffer.graph.Graph;
import gaffer.operation.OperationException;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetAllElements;
import gaffer.store.Store;
import gaffer.store.StoreProperties;
import gaffer.store.schema.Schema;
import gaffer.user.User;
import java.util.HashMap;
import java.util.Map;

public abstract class GafferExporter extends ElementExporter {
    private final Map<String, Graph> graphExports = new HashMap<>();
    private Schema schema;
    private StoreProperties storeProperties;

    public GafferExporter() {
    }

    @Override
    public boolean initialise(final Object storeObj, final User user) {
        final boolean isNew = super.initialise(storeObj, user);
        final Store store = ((Store) storeObj);

        // clone the schema
        schema = store.getSchema().clone();

        // clone the store properties
        storeProperties = store.getProperties().clone();

        return isNew;
    }

    protected Map<String, Graph> getGraphExports() {
        return graphExports;
    }

    @Override
    protected void addElements(final String key, final Iterable<Element> elements, final User user) {
        validateAdd(key, elements, user);
        final Graph graph = getGraphExport(key);
        try {
            graph.execute(new AddElements.Builder()
                    .elements(elements)
                    .build(), user);
        } catch (OperationException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    protected CloseableIterable<Element> getElements(final String key, final User user, final int start, final int end) {
        validateGet(key, user, start, end);

        final Graph graph = getGraphExport(key);
        try {
            return new LimitedClosableIterable<>(graph.execute(new GetAllElements<>(), user), start, end);
        } catch (OperationException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected Graph getGraphExport(final String key) {
        final String userExportKey = getUserExportKey(key);
        Graph graph = graphExports.get(userExportKey);
        if (null == graph) {
            graph = new Graph.Builder()
                    .addSchema(schema)
                    .storeProperties(createExportStorePropsWithKey(key))
                    .build();
            graphExports.put(userExportKey, graph);
        }

        return graph;
    }

    protected Schema getSchema() {
        return schema;
    }

    protected StoreProperties getStoreProperties() {
        return storeProperties;
    }

    /**
     * This method should ensure the new store
     * separates the exported elements from the main storage.
     * By default it just uses a clone of the main properties.
     *
     * @param key the export key
     * @return the new store properties for the new gaffer graph export
     */
    protected abstract StoreProperties createExportStorePropsWithKey(final String key);

    protected abstract void validateAdd(final String key, final Iterable<Element> elements, final User user);

    protected abstract void validateGet(final String key, final User user, final int start, final int end);
}
