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

package gaffer.graph;


import gaffer.data.elementdefinition.view.View;
import gaffer.operation.Operation;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.store.Store;
import gaffer.store.StoreException;
import gaffer.store.StoreProperties;
import gaffer.store.StoreTrait;
import gaffer.store.schema.DataSchema;
import gaffer.data.elementdefinition.exception.SchemaException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The Graph separates the user from the {@link Store}. It holds an instance of the {@link Store} and
 * acts as a proxy for the store, delegating {@link Operation}s to the store.
 * <p>
 * The Graph provides users with a single point of entry for executing operations on a store.
 * This allows the underlying store to be swapped and the same operations can still be applied.
 * <p>
 * Graphs also provides a view of the data with a instance of {@link View}. The view filters out unwanted information
 * and can transform {@link gaffer.data.element.Properties} into transient properties such as averages.
 * <p>
 * When executing operations on a graph, an operation view would override the graph view.
 */
public final class Graph {

    /**
     * The instance of the store.
     */
    private final Store store;

    /**
     * The {@link gaffer.data.elementdefinition.view.View} - by default this will just contain all the groups
     * in the graph's {@link gaffer.store.schema.DataSchema}, however it can be set to a subview to
     * allow multiple operations to be performed on the same subview.
     */
    private final View view;

    /**
     * Constructs a <code>Graph</code> with the {@link java.nio.file.Path}s to the various JSON schemas and
     * the store property file.
     * <p>
     * A full graph {@link gaffer.data.elementdefinition.view.View} will be automatically generated based on the
     * {@link gaffer.store.schema.DataSchema}, i.e no filtering or transformations will be done.
     *
     * @param storePropertiesPath   a {@link java.nio.file.Path} to the store properties
     * @param dataSchemaModulePaths {@link java.nio.file.Path}s to {@link DataSchema} modules
     * @throws SchemaException thrown if the {@link gaffer.store.schema.DataSchema} or
     *                         {@link gaffer.store.schema.DataSchema} is invalid
     */
    public Graph(final Path storePropertiesPath, final Path... dataSchemaModulePaths) throws SchemaException {
        this(storePropertiesPath, (View) null, dataSchemaModulePaths);
    }

    /**
     * Constructs a <code>Graph</code> with the {@link java.nio.file.Path}s to the various JSON schemas, the store
     * property file and a JSON graph {@link gaffer.data.elementdefinition.view.View}.
     *
     * @param storePropertiesPath   a {@link java.nio.file.Path} to the store properties
     * @param view                  a {@link java.nio.file.Path} to the JSON {@link gaffer.data.elementdefinition.view.View}
     * @param dataSchemaModulePaths {@link java.nio.file.Path}s to {@link DataSchema} modules
     * @throws SchemaException thrown if the {@link gaffer.store.schema.DataSchema} or
     *                         {@link gaffer.store.schema.DataSchema} is invalid
     */
    public Graph(final Path storePropertiesPath, final View view,
                 final Path... dataSchemaModulePaths) throws SchemaException {
        this(createInputStream(storePropertiesPath), view,
                createInputStreams(dataSchemaModulePaths));
    }

    /**
     * Constructs a <code>Graph</code> with the {@link java.io.InputStream}s for the various JSON schemas and
     * the store property file.
     * <p>
     * A full graph {@link gaffer.data.elementdefinition.view.View} will be automatically generated based on the
     * {@link gaffer.store.schema.DataSchema}, i.e no filtering or transformations will be done.
     *
     * @param storePropertiesStream   a {@link java.io.InputStream} for the store properties
     * @param dataSchemaModuleStreams {@link java.io.InputStream}s to {@link DataSchema} modules
     * @throws SchemaException thrown if the {@link gaffer.store.schema.DataSchema} or
     *                         {@link gaffer.store.schema.DataSchema} is invalid
     */
    public Graph(final InputStream storePropertiesStream,
                 final InputStream... dataSchemaModuleStreams) throws SchemaException {
        this(storePropertiesStream, (View) null, dataSchemaModuleStreams);
    }

    /**
     * Constructs a <code>Graph</code> with the {@link java.io.InputStream}s for the various JSON schemas, the store
     * property file and a JSON graph {@link gaffer.data.elementdefinition.view.View}.
     *
     * @param storePropertiesStream   a {@link java.io.InputStream} for the store properties
     * @param view                    a {@link java.io.InputStream}  to the JSON {@link gaffer.data.elementdefinition.view.View}
     * @param dataSchemaModuleStreams {@link java.io.InputStream}s to {@link DataSchema} modules
     * @throws SchemaException thrown if the {@link gaffer.store.schema.DataSchema} or
     *                         {@link gaffer.store.schema.DataSchema} is invalid
     */
    public Graph(final InputStream storePropertiesStream, final View view,
                 final InputStream... dataSchemaModuleStreams) throws SchemaException {
        this(createStore(storePropertiesStream, dataSchemaModuleStreams), view);
    }

    /**
     * Constructs a <code>Graph</code> with the various schemas and the store property file.
     * <p>
     * A full graph {@link gaffer.data.elementdefinition.view.View} will be automatically generated based on the
     * {@link gaffer.store.schema.DataSchema}, i.e no filtering or transformations will be done.
     *
     * @param storeProperties   the {@link gaffer.store.StoreProperties}
     * @param dataSchemaModules additional {@link DataSchema} modules
     * @throws SchemaException thrown if the {@link gaffer.store.schema.DataSchema} or
     *                         {@link gaffer.store.schema.DataSchema} is invalid
     */
    public Graph(final StoreProperties storeProperties, final DataSchema... dataSchemaModules)
            throws SchemaException {
        this(storeProperties, null, dataSchemaModules);
    }

    /**
     * Constructs a <code>Graph</code> with the various schemas, the store property file and a JSON graph
     * {@link gaffer.data.elementdefinition.view.View}.
     *
     * @param storeProperties   the {@link gaffer.store.StoreProperties}
     * @param view              a graph {@link gaffer.data.elementdefinition.view.View}
     * @param dataSchemaModules {@link DataSchema} modules
     * @throws SchemaException thrown if the {@link gaffer.store.schema.DataSchema} or
     *                         {@link gaffer.store.schema.DataSchema} is invalid
     */
    public Graph(final StoreProperties storeProperties, final View view,
                 final DataSchema... dataSchemaModules) throws SchemaException {
        this(createStore(storeProperties, dataSchemaModules), view);
    }

    /**
     * Constructs a <code>Graph</code> with the given {@link gaffer.store.Store}.
     * <p>
     * A full graph {@link gaffer.data.elementdefinition.view.View} will be automatically generated based on the
     * {@link gaffer.store.schema.DataSchema}, i.e no filtering or transformations will be done.
     *
     * @param store an instance of {@link Store} used to store the elements and handle operations.
     */
    public Graph(final Store store) {
        this(store, null);
    }

    /**
     * Constructs a <code>Graph</code> with the given {@link gaffer.store.Store} and
     * {@link gaffer.data.elementdefinition.view.View}.
     *
     * @param store a {@link Store} used to store the elements and handle operations.
     * @param view  a {@link View} defining the view of the data for the graph.
     */
    public Graph(final Store store, final View view) {
        this.store = store;
        if (null == view) {
            this.view = new View(store.getDataSchema().getEntityGroups(), store.getDataSchema().getEdgeGroups());
        } else {
            this.view = view;
        }
    }

    /**
     * Performs the given operation on the store.
     * If the operation does not have a view then the graph view is used.
     *
     * @param operation the operation to be executed.
     * @param <OUTPUT>  the operation output type.
     * @return the operation result.
     * @throws OperationException if an operation fails
     */
    public <OUTPUT> OUTPUT execute(final Operation<?, OUTPUT> operation) throws OperationException {
        return execute(new OperationChain<>(operation));
    }

    /**
     * Performs the given operation on the store.
     * If the operation does not have a view then the graph view is used.
     *
     * @param operationChain the operation chain to be executed.
     * @param <OUTPUT>       the operation chain output type.
     * @return the operation result.
     * @throws OperationException if an operation fails
     */
    public <OUTPUT> OUTPUT execute(final OperationChain<OUTPUT> operationChain) throws OperationException {
        for (Operation operation : operationChain.getOperations()) {
            if (null == operation.getView()) {
                operation.setView(view);
            }
        }

        return store.execute(operationChain);
    }

    /**
     * Returns the graph view.
     *
     * @return the graph view.
     */
    public View getView() {
        return view;
    }

    /**
     * @return the data schema.
     */
    public DataSchema getDataSchema() {
        return store.getDataSchema();
    }

    /**
     * @param storeTrait the store trait to check
     * @return true if the store has the given trait.
     */
    public boolean hasTrait(final StoreTrait storeTrait) {
        return store.hasTrait(storeTrait);
    }

    private static Store createStore(final InputStream storePropertiesStream,
                                     final InputStream... dataSchemaStreams) {
        if (null == dataSchemaStreams || 0 == dataSchemaStreams.length) {
            throw new IllegalArgumentException("At least one data schema module is required");
        }

        final StoreProperties storeProperties = StoreProperties.loadStoreProperties(storePropertiesStream);
        final Class<? extends DataSchema> dataSchemaClass;
        try {
            dataSchemaClass = Class.forName(storeProperties.getDataSchemaClass()).asSubclass(DataSchema.class);
        } catch (ClassNotFoundException e) {
            throw new SchemaException("Data schema class was not found: " + storeProperties.getDataSchemaClass(), e);
        }

        return createStore(storeProperties, DataSchema.fromJson(dataSchemaClass, dataSchemaStreams));
    }

    private static Store createStore(final StoreProperties storeProperties,
                                     final DataSchema... dataSchemaModules) {
        if (null == dataSchemaModules || 0 == dataSchemaModules.length) {
            throw new IllegalArgumentException("At least one data schema module is required");
        }

        DataSchema mergedDataSchema = null;
        for (DataSchema dataSchemaModule : dataSchemaModules) {
            if (null == mergedDataSchema) {
                mergedDataSchema = dataSchemaModule;
            } else {
                mergedDataSchema.merge(dataSchemaModule);
            }
        }

        return createStore(storeProperties, mergedDataSchema);
    }

    private static Store createStore(final StoreProperties storeProperties, final DataSchema dataSchema) {
        final String storeClass = storeProperties.getStoreClass();
        if (null == storeClass) {
            throw new IllegalArgumentException("The Store class name was not found in the store properties for key: " + StoreProperties.STORE_PROPERTIES_CLASS);
        }

        final Store newStore;
        try {
            newStore = Class.forName(storeClass).asSubclass(Store.class).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not create store of type: " + storeClass);
        }

        try {
            newStore.initialise(dataSchema, storeProperties);
        } catch (StoreException e) {
            throw new IllegalArgumentException("Could not initialise the store with provided arguments.", e);
        }
        return newStore;
    }

    private static InputStream createInputStream(final Path path) {
        try {
            return null != path ? Files.newInputStream(path) : null;
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to create input stream from path: " + path, e);
        }
    }

    private static InputStream[] createInputStreams(final Path... paths) {
        InputStream[] stream = new InputStream[paths.length];
        for (int i = 0; i < paths.length; i++) {
            try {
                stream[i] = null != paths[i] ? Files.newInputStream(paths[i]) : null;
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to create input stream from path: " + paths[i], e);
            }
        }

        return stream;
    }
}
