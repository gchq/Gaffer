/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.accumulostore.inputformat.ElementInputFormat;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloKeyPackage;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.AddElementsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetAdjacentEntitySeedsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetAllElementsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetElementsBetweenSetsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetElementsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetElementsInRangesHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetElementsWithinSetHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.SummariseGroupOverRangesHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.AddElementsFromHdfsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.ImportAccumuloKeyValueFilesHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.SampleDataForSplitPointsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.SplitTableHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation.ImportAccumuloKeyValueFiles;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation.SplitTable;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetEdgesBetweenSets;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetEdgesInRanges;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetEdgesWithinSet;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsBetweenSets;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsInRanges;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsWithinSet;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetEntitiesInRanges;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.SummariseGroupOverRanges;
import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.accumulostore.utils.TableUtils;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.core.exception.Status;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static uk.gov.gchq.gaffer.store.StoreTrait.ORDERED;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_TRANSFORMATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.QUERY_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.STORE_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.STORE_VALIDATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.TRANSFORMATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.VISIBILITY;

/**
 * An Accumulo Implementation of the Gaffer Framework
 * <p>
 * The key detail of the Accumulo implementation is that any Edge inserted by a
 * user is inserted into the accumulo table twice, once with the source object
 * being put first in the key and once with the destination bring put first in
 * the key This is to enable an edge to be found in a Range scan when providing
 * only one end of the edge.
 */
public class AccumuloStore extends Store {
    public static final Set<StoreTrait> TRAITS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(STORE_AGGREGATION, QUERY_AGGREGATION, PRE_AGGREGATION_FILTERING, POST_AGGREGATION_FILTERING, POST_TRANSFORMATION_FILTERING, TRANSFORMATION, STORE_VALIDATION, ORDERED, VISIBILITY)));
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloStore.class);
    private AccumuloKeyPackage keyPackage;
    private Connector connection = null;

    @Override
    public void initialise(final Schema schema, final StoreProperties properties)
            throws StoreException {
        super.initialise(schema, properties);
        final String keyPackageClass = getProperties().getKeyPackageClass();
        try {
            this.keyPackage = Class.forName(keyPackageClass).asSubclass(AccumuloKeyPackage.class).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new StoreException("Unable to construct an instance of key package: " + keyPackageClass, e);
        }
        this.keyPackage.setSchema(getSchema());
        TableUtils.ensureTableExists(this);
    }

    /**
     * Creates an Accumulo {@link org.apache.accumulo.core.client.Connector}
     * using the properties found in properties file associated with the
     * AccumuloStore
     *
     * @return A new {@link Connector}
     * @throws StoreException if there is a failure to connect to accumulo.
     */
    public Connector getConnection() throws StoreException {
        if (null == connection) {
            connection = TableUtils.getConnector(getProperties().getInstance(), getProperties().getZookeepers(),
                    getProperties().getUser(), getProperties().getPassword());
        }
        return connection;
    }

    /**
     * Updates a Hadoop {@link Configuration} with information needed to connect to the Accumulo store. It adds
     * iterators to apply the provided {@link View}. This method will be used by operations that run MapReduce
     * or Spark jobs against the Accumulo store.
     *
     * @param conf A {@link Configuration} to be updated.
     * @param view The {@link View} to be applied.
     * @param user The {@link User} to be used.
     * @throws StoreException if there is a failure to connect to Accumulo or a problem setting the iterators.
     */
    public void updateConfiguration(final Configuration conf, final View view, final User user) throws StoreException {
        try {
            // Table name
            InputConfigurator.setInputTableName(AccumuloInputFormat.class,
                    conf,
                    getProperties().getTable());
            // User
            addUserToConfiguration(conf);
            // Authorizations
            Authorizations authorisations;
            if (null != user && null != user.getDataAuths()) {
                authorisations = new Authorizations(user.getDataAuths().toArray(new String[user.getDataAuths().size()]));
            } else {
                authorisations = new Authorizations();
            }
            InputConfigurator.setScanAuthorizations(AccumuloInputFormat.class,
                    conf,
                    authorisations);
            // Zookeeper
            addZookeeperToConfiguration(conf);
            // Add keypackage, schema and view to conf
            conf.set(ElementInputFormat.KEY_PACKAGE, getProperties().getKeyPackageClass());
            conf.set(ElementInputFormat.SCHEMA, new String(getSchema().toCompactJson(), CommonConstants.UTF_8));
            conf.set(ElementInputFormat.VIEW, new String(view.toCompactJson(), CommonConstants.UTF_8));
            // Add iterators that depend on the view
            if (view.hasGroups()) {
                IteratorSetting elementPreFilter = getKeyPackage()
                        .getIteratorFactory()
                        .getElementPreAggregationFilterIteratorSetting(view, this);
                IteratorSetting elementPostFilter = getKeyPackage()
                        .getIteratorFactory()
                        .getElementPostAggregationFilterIteratorSetting(view, this);
                InputConfigurator.addIterator(AccumuloInputFormat.class, conf, elementPostFilter);
                InputConfigurator.addIterator(AccumuloInputFormat.class, conf, elementPreFilter);
            }
        } catch (final AccumuloSecurityException | IteratorSettingException | UnsupportedEncodingException e) {
            throw new StoreException(e);
        }
    }

    protected void addUserToConfiguration(final Configuration conf) throws AccumuloSecurityException {
        InputConfigurator.setConnectorInfo(AccumuloInputFormat.class,
                conf,
                getProperties().getUser(),
                new PasswordToken(getProperties().getPassword()));
    }

    protected void addZookeeperToConfiguration(final Configuration conf) {
        InputConfigurator.setZooKeeperInstance(AccumuloInputFormat.class,
                conf,
                new ClientConfiguration()
                        .withInstance(getProperties().getInstance())
                        .withZkHosts(getProperties().getZookeepers()));
    }

    @Override
    public <OUTPUT> OUTPUT doUnhandledOperation(final Operation<?, OUTPUT> operation, final Context context) {
        throw new UnsupportedOperationException("Operation: " + operation.getClass() + " is not supported");
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "The properties should always be AccumuloProperties")
    @Override
    public AccumuloProperties getProperties() {
        return (AccumuloProperties) super.getProperties();
    }

    @Override
    protected void addAdditionalOperationHandlers() {
        addOperationHandler(AddElementsFromHdfs.class, new AddElementsFromHdfsHandler());
        addOperationHandler(GetEdgesBetweenSets.class, new GetElementsBetweenSetsHandler());
        addOperationHandler(GetElementsBetweenSets.class, new GetElementsBetweenSetsHandler());
        addOperationHandler(GetEdgesInRanges.class, new GetElementsInRangesHandler());
        addOperationHandler(GetElementsInRanges.class, new GetElementsInRangesHandler());
        addOperationHandler(GetEntitiesInRanges.class, new GetElementsInRangesHandler());
        addOperationHandler(GetElementsWithinSet.class, new GetElementsWithinSetHandler());
        addOperationHandler(GetEdgesWithinSet.class, new GetElementsWithinSetHandler());
        addOperationHandler(SplitTable.class, new SplitTableHandler());
        addOperationHandler(SampleDataForSplitPoints.class, new SampleDataForSplitPointsHandler());
        addOperationHandler(ImportAccumuloKeyValueFiles.class, new ImportAccumuloKeyValueFilesHandler());
        addOperationHandler(SummariseGroupOverRanges.class, new SummariseGroupOverRangesHandler());
    }

    @Override
    protected OperationHandler<GetElements<ElementSeed, Element>, CloseableIterable<Element>> getGetElementsHandler() {
        return new GetElementsHandler();
    }

    @Override
    protected OperationHandler<GetAllElements<Element>, CloseableIterable<Element>> getGetAllElementsHandler() {
        return new GetAllElementsHandler();
    }

    @Override
    protected OperationHandler<? extends GetAdjacentEntitySeeds, CloseableIterable<EntitySeed>> getAdjacentEntitySeedsHandler() {
        return new GetAdjacentEntitySeedsHandler();
    }

    @Override
    protected OperationHandler<? extends AddElements, Void> getAddElementsHandler() {
        return new AddElementsHandler();
    }

    @Override
    public Set<StoreTrait> getTraits() {
        return TRAITS;
    }

    /**
     * Method to add {@link Element}s into Accumulo
     *
     * @param elements the elements to be added
     * @throws StoreException failure to insert the elements into a table
     */
    public void addElements(final Iterable<Element> elements) throws StoreException {
        insertGraphElements(elements);
    }

    protected void insertGraphElements(final Iterable<Element> elements) throws StoreException {
        // Create BatchWriter
        final BatchWriter writer = TableUtils.createBatchWriter(this);
        // Loop through elements, convert to mutations, and add to
        // BatchWriter.as
        // The BatchWriter takes care of batching them up, sending them without
        // too high a latency, etc.
        if (elements != null) {
            for (final Element element : elements) {
                final Pair<Key> keys;
                try {
                    keys = keyPackage.getKeyConverter().getKeysFromElement(element);
                } catch (final AccumuloElementConversionException e) {
                    LOGGER.error("Failed to create an accumulo key from element of type " + element.getGroup()
                            + " when trying to insert elements");
                    continue;
                }
                final Value value;
                try {
                    value = keyPackage.getKeyConverter().getValueFromElement(element);
                } catch (final AccumuloElementConversionException e) {
                    LOGGER.error("Failed to create an accumulo value from element of type " + element.getGroup()
                            + " when trying to insert elements");
                    continue;
                }
                final Mutation m = new Mutation(keys.getFirst().getRow());
                m.put(keys.getFirst().getColumnFamily(), keys.getFirst().getColumnQualifier(),
                        new ColumnVisibility(keys.getFirst().getColumnVisibility()), keys.getFirst().getTimestamp(), value);
                try {
                    writer.addMutation(m);
                } catch (final MutationsRejectedException e) {
                    LOGGER.error("Failed to create an accumulo key mutation");
                    continue;
                }
                // If the GraphElement is a Vertex then there will only be 1 key,
                // and the second will be null.
                // If the GraphElement is an Edge then there will be 2 keys.
                if (keys.getSecond() != null) {
                    final Mutation m2 = new Mutation(keys.getSecond().getRow());
                    m2.put(keys.getSecond().getColumnFamily(), keys.getSecond().getColumnQualifier(),
                            new ColumnVisibility(keys.getSecond().getColumnVisibility()), keys.getSecond().getTimestamp(),
                            value);
                    try {
                        writer.addMutation(m2);
                    } catch (final MutationsRejectedException e) {
                        LOGGER.error("Failed to create an accumulo key mutation");
                    }
                }
            }
        } else {
            throw new GafferRuntimeException("Could not find any elements to add to graph.", Status.BAD_REQUEST);
        }
        try {
            writer.close();
        } catch (final MutationsRejectedException e) {
            LOGGER.warn("Accumulo batch writer failed to close", e);
        }
    }

    /**
     * Returns the {@link uk.gov.gchq.gaffer.accumulostore.key.AccumuloKeyPackage} in use by
     * this AccumuloStore.
     *
     * @return {@link uk.gov.gchq.gaffer.accumulostore.key.AccumuloKeyPackage}
     */
    public AccumuloKeyPackage getKeyPackage() {
        return keyPackage;
    }

    @Override
    public boolean isValidationRequired() {
        return false;
    }
}
