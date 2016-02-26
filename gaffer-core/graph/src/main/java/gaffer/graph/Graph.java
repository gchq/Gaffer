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


import gaffer.data.elementdefinition.schema.exception.SchemaException;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.Operation;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.store.Store;
import gaffer.store.StoreException;
import gaffer.store.StoreProperties;
import gaffer.store.StoreTrait;
import gaffer.store.schema.DataSchema;
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
     * @param dataSchemaPath      a {@link java.nio.file.Path} to the JSON
     *                            {@link gaffer.store.schema.DataSchema}
     * @param storePropertiesPath a {@link java.nio.file.Path} to the store properties
     * @throws SchemaException thrown if the {@link gaffer.store.schema.DataSchema} or
     *                         {@link gaffer.store.schema.DataSchema} is invalid
     */
    public Graph(final Path dataSchemaPath, final Path storePropertiesPath)
            throws SchemaException {
        this(dataSchemaPath, storePropertiesPath, (View) null);
    }

    /**
     * Constructs a <code>Graph</code> with the {@link java.nio.file.Path}s to the various JSON schemas and
     * the store property file.
     * <p>
     * A full graph {@link gaffer.data.elementdefinition.view.View} will be automatically generated based on the
     * {@link gaffer.store.schema.DataSchema}, i.e no filtering or transformations will be done.
     *
     * @param dataSchemaPath      a {@link java.nio.file.Path} to the JSON
     *                            {@link gaffer.store.schema.DataSchema}
     * @param storePropertiesPath a {@link java.nio.file.Path} to the store properties
     * @param schemaTypesPath     a {@link java.nio.file.Path} to the JSON {@link gaffer.store.schema.Types}
     * @throws SchemaException thrown if the {@link gaffer.store.schema.DataSchema} or
     *                         {@link gaffer.store.schema.DataSchema} is invalid
     */
    public Graph(final Path dataSchemaPath, final Path storePropertiesPath,
                 final Path schemaTypesPath) throws SchemaException {
        this(dataSchemaPath, storePropertiesPath, schemaTypesPath, null);
    }

    /**
     * Constructs a <code>Graph</code> with the {@link java.nio.file.Path}s to the various JSON schemas, the store
     * property file and a JSON graph {@link gaffer.data.elementdefinition.view.View}.
     *
     * @param dataSchemaPath      a {@link java.nio.file.Path} to the JSON
     *                            {@link gaffer.store.schema.DataSchema}
     * @param storePropertiesPath a {@link java.nio.file.Path} to the store properties
     * @param view                a {@link java.nio.file.Path} to the JSON {@link gaffer.data.elementdefinition.view.View}
     * @throws SchemaException thrown if the {@link gaffer.store.schema.DataSchema} or
     *                         {@link gaffer.store.schema.DataSchema} is invalid
     */
    public Graph(final Path dataSchemaPath, final Path storePropertiesPath, final View view)
            throws SchemaException {
        this(dataSchemaPath, storePropertiesPath, null, view);
    }

    /**
     * Constructs a <code>Graph</code> with the {@link java.nio.file.Path}s to the various JSON schemas, the store
     * property file and a JSON graph {@link gaffer.data.elementdefinition.view.View}.
     *
     * @param dataSchemaPath      a {@link java.nio.file.Path} to the JSON
     *                            {@link gaffer.store.schema.DataSchema}
     * @param storePropertiesPath a {@link java.nio.file.Path} to the store properties
     * @param schemaTypesPath     a {@link java.nio.file.Path} to the JSON {@link gaffer.store.schema.Types}
     * @param view                a {@link java.nio.file.Path} to the JSON {@link gaffer.data.elementdefinition.view.View}
     * @throws SchemaException thrown if the {@link gaffer.store.schema.DataSchema} or
     *                         {@link gaffer.store.schema.DataSchema} is invalid
     */
    public Graph(final Path dataSchemaPath, final Path storePropertiesPath,
                 final Path schemaTypesPath, final View view) throws SchemaException {
        this(createInputStream(dataSchemaPath),
                createInputStream(storePropertiesPath), createInputStream(schemaTypesPath), view);
    }

    /**
     * Constructs a <code>Graph</code> with the {@link java.io.InputStream}s for the various JSON schemas and
     * the store property file.
     * <p>
     * A full graph {@link gaffer.data.elementdefinition.view.View} will be automatically generated based on the
     * {@link gaffer.store.schema.DataSchema}, i.e no filtering or transformations will be done.
     *
     * @param dataSchemaStream      a {@link java.io.InputStream} for the JSON
     *                              {@link gaffer.store.schema.DataSchema}
     * @param storePropertiesStream a {@link java.io.InputStream} for the store properties
     * @throws SchemaException thrown if the {@link gaffer.store.schema.DataSchema} or
     *                         {@link gaffer.store.schema.DataSchema} is invalid
     */
    public Graph(final InputStream dataSchemaStream,
                 final InputStream storePropertiesStream) throws SchemaException {
        this(dataSchemaStream, storePropertiesStream, (View) null);
    }

    /**
     * Constructs a <code>Graph</code> with the {@link java.io.InputStream}s for the various JSON schemas and
     * the store property file.
     * <p>
     * A full graph {@link gaffer.data.elementdefinition.view.View} will be automatically generated based on the
     * {@link gaffer.store.schema.DataSchema}, i.e no filtering or transformations will be done.
     *
     * @param dataSchemaStream      a {@link java.io.InputStream} for the JSON
     *                              {@link gaffer.store.schema.DataSchema}
     * @param storePropertiesStream a {@link java.io.InputStream} for the store properties
     * @param schemaTypesStream     a {@link java.io.InputStream} for the JSON {@link gaffer.store.schema.Types}
     * @throws SchemaException thrown if the {@link gaffer.store.schema.DataSchema} or
     *                         {@link gaffer.store.schema.DataSchema} is invalid
     */
    public Graph(final InputStream dataSchemaStream,
                 final InputStream storePropertiesStream, final InputStream schemaTypesStream) throws SchemaException {
        this(dataSchemaStream, storePropertiesStream, schemaTypesStream, null);
    }

    /**
     * Constructs a <code>Graph</code> with the {@link java.io.InputStream}s for the various JSON schemas, the store
     * property file and a JSON graph {@link gaffer.data.elementdefinition.view.View}.
     *
     * @param dataSchemaStream      a {@link java.io.InputStream} for the JSON
     *                              {@link gaffer.store.schema.DataSchema}
     * @param storePropertiesStream a {@link java.io.InputStream} for the store properties
     * @param view                  a {@link java.io.InputStream}  to the JSON {@link gaffer.data.elementdefinition.view.View}
     * @throws SchemaException thrown if the {@link gaffer.store.schema.DataSchema} or
     *                         {@link gaffer.store.schema.DataSchema} is invalid
     */
    public Graph(final InputStream dataSchemaStream,
                 final InputStream storePropertiesStream, final View view) throws SchemaException {
        this(dataSchemaStream, storePropertiesStream, null, view);
    }

    /**
     * Constructs a <code>Graph</code> with the {@link java.io.InputStream}s for the various JSON schemas, the store
     * property file and a JSON graph {@link gaffer.data.elementdefinition.view.View}.
     *
     * @param dataSchemaStream      a {@link java.io.InputStream} for the JSON
     *                              {@link gaffer.store.schema.DataSchema}
     * @param storePropertiesStream a {@link java.io.InputStream} for the store properties
     * @param schemaTypesStream     a {@link java.io.InputStream} for the JSON {@link gaffer.store.schema.Types}
     * @param view                  a {@link java.io.InputStream}  to the JSON {@link gaffer.data.elementdefinition.view.View}
     * @throws SchemaException thrown if the {@link gaffer.store.schema.DataSchema} or
     *                         {@link gaffer.store.schema.DataSchema} is invalid
     */
    public Graph(final InputStream dataSchemaStream,
                 final InputStream storePropertiesStream, final InputStream schemaTypesStream, final View view) throws SchemaException {
        this(createStore(dataSchemaStream, storePropertiesStream, schemaTypesStream), view);
    }

    /**
     * Constructs a <code>Graph</code> with the various schemas and the store property file.
     * <p>
     * A full graph {@link gaffer.data.elementdefinition.view.View} will be automatically generated based on the
     * {@link gaffer.store.schema.DataSchema}, i.e no filtering or transformations will be done.
     *
     * @param dataSchema      the {@link gaffer.store.schema.DataSchema}
     * @param storeProperties the {@link gaffer.store.StoreProperties}
     * @throws SchemaException thrown if the {@link gaffer.store.schema.DataSchema} or
     *                         {@link gaffer.store.schema.DataSchema} is invalid
     */
    public Graph(final DataSchema dataSchema, final StoreProperties storeProperties)
            throws SchemaException {
        this(dataSchema, storeProperties, null);
    }

    /**
     * Constructs a <code>Graph</code> with the various schemas, the store property file and a JSON graph
     * {@link gaffer.data.elementdefinition.view.View}.
     *
     * @param dataSchema      the {@link gaffer.store.schema.DataSchema}
     * @param storeProperties the {@link gaffer.store.StoreProperties}
     * @param view            a graph {@link gaffer.data.elementdefinition.view.View}
     * @throws SchemaException thrown if the {@link gaffer.store.schema.DataSchema} or
     *                         {@link gaffer.store.schema.DataSchema} is invalid
     */
    public Graph(final DataSchema dataSchema, final StoreProperties storeProperties,
                 final View view)
            throws SchemaException {
        this(createStore(dataSchema, storeProperties), view);
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

    private static Store createStore(final InputStream dataSchemaStream,
                                     final InputStream storePropertiesStream,
                                     final InputStream schemaTypesStream) {
        final StoreProperties storeProperties = StoreProperties.loadStoreProperties(storePropertiesStream);
        DataSchema dataSchema = loadDataSchema(dataSchemaStream, schemaTypesStream, storeProperties.getDataSchemaClass());
        return createStore(dataSchema, storeProperties);
    }

    private static Store createStore(final DataSchema dataSchema, final StoreProperties storeProperties) {
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

    private static DataSchema loadDataSchema(final InputStream dataSchemaStream, final InputStream typeStream, final String dataSchemaClass) throws SchemaException {
        final DataSchema dataSchema;

        try {
            dataSchema = DataSchema.fromJson(dataSchemaStream, Class.forName(dataSchemaClass).asSubclass(DataSchema.class));
        } catch (ClassNotFoundException e) {
            throw new SchemaException("Store schema class was not found: " + dataSchemaClass, e);
        }

        if (null != typeStream) {
            dataSchema.addTypesFromStream(typeStream);
        }

        if (!dataSchema.validate()) {
            throw new SchemaException("ERROR: store schema failed to validate. Please check the logs for more information");
        }

        return dataSchema;
    }

    private static InputStream createInputStream(final Path path) {
        try {
            return null != path ? Files.newInputStream(path) : null;
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to create input stream from path: " + path, e);
        }
    }
}
