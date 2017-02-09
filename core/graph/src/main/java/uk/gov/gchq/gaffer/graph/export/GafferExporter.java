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

package uk.gov.gchq.gaffer.graph.export;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.LimitedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.export.ElementExporter;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import java.util.HashMap;
import java.util.Map;

public abstract class GafferExporter extends ElementExporter<Store> {
    private final Map<String, Graph> graphExports = new HashMap<>();
    private Schema schema;
    private StoreProperties storeProperties;

    public GafferExporter() {
    }

    @Override
    public void initialise(final String key, final Store store, final User user) {
        super.initialise(key, store, user);

        // clone the schema
        schema = store.getSchema().clone();

        // clone the store properties
        storeProperties = store.getProperties().clone();
    }

    protected Map<String, Graph> getGraphExports() {
        return graphExports;
    }

    @Override
    protected void addElements(final Iterable<Element> elements, final User user) {
        validateAdd(elements, user);
        final Graph graph = getGraphExport();
        try {
            graph.execute(new AddElements.Builder()
                    .elements(elements)
                    .build(), user);
        } catch (OperationException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    protected CloseableIterable<Element> getElements(final User user, final int start, final int end) {
        validateGet(user, start, end);

        final Graph graph = getGraphExport();
        try {
            return new LimitedCloseableIterable<>(graph.execute(new GetAllElements<>(), user), start, end);
        } catch (OperationException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected Graph getGraphExport() {
        final String userExportKey = getExportName();
        Graph graph = graphExports.get(userExportKey);
        if (null == graph) {
            graph = new Graph.Builder()
                    .addSchema(schema)
                    .storeProperties(createExportStoreProps())
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
     * @return the new store properties for the new gaffer graph export
     */
    protected abstract StoreProperties createExportStoreProps();

    protected abstract void validateAdd(final Iterable<Element> elements, final User user);

    protected abstract void validateGet(final User user, final int start, final int end);
}
