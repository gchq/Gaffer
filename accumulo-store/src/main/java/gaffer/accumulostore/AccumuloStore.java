/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gaffer.accumulostore;

import gaffer.accumulostore.key.AccumuloKeyPackage;
import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.accumulostore.operation.handler.AddElementsHandler;
import gaffer.accumulostore.operation.handler.GetAdjacentEntitySeedsHandler;
import gaffer.accumulostore.operation.handler.GetElementsHandler;
import gaffer.accumulostore.operation.hdfs.handler.AddElementsFromHdfsHandler;
import gaffer.accumulostore.utils.Constants;
import gaffer.accumulostore.utils.Pair;
import gaffer.accumulostore.utils.TableUtilException;
import gaffer.accumulostore.utils.TableUtils;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.operation.Operation;
import gaffer.operation.OperationException;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.operation.impl.get.GetElements;
import gaffer.operation.simple.hdfs.AddElementsFromHdfs;
import gaffer.store.Store;
import gaffer.store.StoreException;
import gaffer.store.StoreProperties;
import gaffer.store.StoreTrait;
import gaffer.store.operation.handler.OperationHandler;
import gaffer.store.schema.StoreSchema;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static gaffer.store.StoreTrait.AGGREGATION;
import static gaffer.store.StoreTrait.FILTERING;
import static gaffer.store.StoreTrait.TRANSFORMATION;
import static gaffer.store.StoreTrait.VALIDATION;

/**
 * An Accumulo Implementation of the Gaffer Framework
 * <p/>
 * The gaffer.accumulostore.key detail of the Accumulo implementation is that any Edge inserted by a user is inserted
 * into the accumulo table twice, once with the source object being put first in the gaffer.accumulostore.key
 * and once with the destination bring put first in the gaffer.accumulostore.key.
 * This is to enable an edge to be found in a Range scan when providing only one end of the edge.
 */
public class AccumuloStore extends Store {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloStore.class);
    private static final List<StoreTrait> TRAITS = Arrays.asList(AGGREGATION, FILTERING, TRANSFORMATION, VALIDATION);
    private AccumuloKeyPackage keyPackage;

    @Override
    public void initialise(final DataSchema dataSchema, final StoreSchema storeSchema, final StoreProperties properties)
            throws StoreException {
        super.initialise(dataSchema, storeSchema, properties);
        final String keyPackageClass = getProperties().getKeyPackageClass();
        try {
            this.keyPackage = Class.forName(keyPackageClass).asSubclass(AccumuloKeyPackage.class).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new StoreException("Unable to construct an instance of key package: " + keyPackageClass);
        }
        this.keyPackage.setStoreSchema(storeSchema);
        validateSchemasAgainstKeyDesign();
    }

    /**
     * Executes a given gaffer.accumulostore.operation and returns the result.
     *
     * @param operation the operation to execute.
     * @param <OUTPUT>  the output type of the operation.
     * @return the result of executing the operation.
     * @throws gaffer.operation.OperationException
     */
    @Override
    protected <OPERATION extends Operation<?, OUTPUT>, OUTPUT> OUTPUT handleOperation(final OPERATION operation) throws OperationException {
        if (operation.getOptions().containsKey(Constants.OPERATION_AUTHORISATIONS)) {
            return super.handleOperation(operation);
        } else {
            throw new OperationException("Operation must have the Authorisations option set");
        }
    }

    /**
     * Creates an Accumulo {@link org.apache.accumulo.core.client.Connector} using the properties found in properties
     * file associated with the AccumuloStore
     *
     * @return A new {@link Connector}
     * @throws gaffer.store.StoreException
     */
    public Connector getConnection() throws StoreException {
        try {
            return TableUtils.getConnector(
                    getProperties().getInstanceName(),
                    getProperties().getZookeepers(),
                    getProperties().getUserName(),
                    getProperties().getPassword()
            );
        } catch (TableUtilException e) {
            throw new StoreException("Failed to create accumulo connection", e);
        }
    }

    @Override
    public <OUTPUT> OUTPUT doUnhandledOperation(final Operation<?, OUTPUT> operation) {
        throw new UnsupportedOperationException("Operation: " + operation.getClass() + " is not supported");
    }

    @Override
    public AccumuloProperties getProperties() {
        return (AccumuloProperties) super.getProperties();
    }

    @Override
    protected void addAdditionalOperationHandlers() {
        addOperationHandler(AddElementsFromHdfs.class, new AddElementsFromHdfsHandler());
    }

    @Override
    protected OperationHandler<GetElements<ElementSeed, Element>, Iterable<Element>> getGetElementsHandler() {
        return new GetElementsHandler();
    }

    @Override
    protected OperationHandler<? extends GetAdjacentEntitySeeds, Iterable<EntitySeed>> getAdjacentEntitySeedsHandler() {
        return new GetAdjacentEntitySeedsHandler();
    }

    @Override
    protected OperationHandler<? extends AddElements, Void> getAddElementsHandler() {
        return new AddElementsHandler();
    }

    @Override
    protected Collection<StoreTrait> getTraits() {
        return TRAITS;
    }

    /**
     * Method to add {@link Element}s into Accumulo
     *
     * @param elements
     */
    public void addElements(final Iterable<Element> elements) throws StoreException {
        try {
            TableUtils.ensureTableExists(this);
        } catch (AccumuloException e) {
            LOGGER.error(e.getMessage(), e);
        }
        insertGraphElements(elements);
    }

    protected void insertGraphElements(final Iterable<Element> elements) throws StoreException {
        // Create BatchWriter
        final BatchWriter writer;
        try {
            writer = TableUtils.createBatchWriter(this);
        } catch (TableUtilException e) {
            throw new StoreException(e);
        }
        // Loop through elements, convert to mutations, and add to BatchWriter.as
        // The BatchWriter takes care of batching them up, sending them without too high a latency, etc.
        for (Element element : elements) {
            final Pair<Key> keys;
            try {
                keys = keyPackage.getKeyConverter().getKeysFromElement(element);
            } catch (AccumuloElementConversionException e) {
                LOGGER.error("Failed to create an accumulo gaffer.accumulostore.key from element of type " + element.getGroup() + " when trying to insert elements");
                continue;
            }
            final Value value;
            try {
                value = keyPackage.getKeyConverter().getValueFromElement(element);
            } catch (AccumuloElementConversionException e) {
                LOGGER.error("Failed to create an accumulo value from element of type " + element.getGroup() + " when trying to insert elements");
                continue;
            }
            final Mutation m = new Mutation(keys.getFirst().getRow());
            m.put(keys.getFirst().getColumnFamily(), keys.getFirst().getColumnQualifier(),
                    new ColumnVisibility(keys.getFirst().getColumnVisibility()), keys.getFirst().getTimestamp(), value);
            try {
                writer.addMutation(m);
            } catch (MutationsRejectedException e) {
                LOGGER.error("Failed to create an accumulo gaffer.accumulostore.key mutation");
                continue;
            }
            // If the GraphElement is a Vertex then there will only be 1 key, and the second will be null.
            // If the GraphElement is an Edge then there will be 2 keys.
            if (keys.getSecond() != null) {
                final Mutation m2 = new Mutation(keys.getSecond().getRow());
                m2.put(keys.getSecond().getColumnFamily(),
                        keys.getSecond().getColumnQualifier(),
                        new ColumnVisibility(keys.getSecond().getColumnVisibility()),
                        keys.getSecond().getTimestamp(),
                        value
                );
                try {
                    writer.addMutation(m2);
                } catch (MutationsRejectedException e) {
                    LOGGER.error("Failed to create an accumulo gaffer.accumulostore.key mutation");
                }
            }
        }
        try {
            writer.close();
        } catch (MutationsRejectedException e) {
            LOGGER.warn("Accumulo batch writer failed to close", e);
        }
    }

    /**
     * Returns the {@link gaffer.accumulostore.key.AccumuloKeyPackage} in use by this AccumuloStore.
     *
     * @return {@link gaffer.accumulostore.key.AccumuloKeyPackage}
     */
    public AccumuloKeyPackage getKeyPackage() {
        return keyPackage;
    }

    protected void validateSchemas() {
        super.validateSchemas();
        final Map<String, String> positions = this.getStoreSchema().getPositions();
        if (positions != null && !positions.isEmpty()) {
            LOGGER.warn("The store schema positions are not used and will be ignored.");
        }
    }

    protected void validateSchemasAgainstKeyDesign() {
        keyPackage.validateSchema(this.getStoreSchema());
    }

    @Override
    public boolean isValidationRequired() {
        return false;
    }
}
