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

import com.google.common.collect.Sets;
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
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.inputformat.ElementInputFormat;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloKeyPackage;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.AddElementsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetAdjacentIdsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetAllElementsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetElementsBetweenSetsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetElementsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetElementsInRangesHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetElementsWithinSetHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.SampleElementsForSplitPointsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.SummariseGroupOverRangesHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.AddElementsFromHdfsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.ImportAccumuloKeyValueFilesHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.SampleDataForSplitPointsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.SplitStoreFromIterableHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.SplitStoreHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation.ImportAccumuloKeyValueFiles;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsBetweenSets;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsInRanges;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsWithinSet;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.SummariseGroupOverRanges;
import uk.gov.gchq.gaffer.accumulostore.utils.TableUtils;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.core.exception.Status;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.hdfs.operation.handler.HdfsSplitStoreFromFileHandler;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.impl.SampleElementsForSplitPoints;
import uk.gov.gchq.gaffer.operation.impl.SplitStore;
import uk.gov.gchq.gaffer.operation.impl.SplitStoreFromFile;
import uk.gov.gchq.gaffer.operation.impl.SplitStoreFromIterable;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaOptimiser;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static uk.gov.gchq.gaffer.store.StoreTrait.INGEST_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.ORDERED;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_TRANSFORMATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.QUERY_AGGREGATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.STORE_VALIDATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.TRANSFORMATION;
import static uk.gov.gchq.gaffer.store.StoreTrait.VISIBILITY;

/**
 * An Accumulo Implementation of the Gaffer Framework
 * <p>
 * The key detail of the Accumulo implementation is that any Edge inserted by a
 * user is inserted into the accumulo table twice, once with the source object
 * being put first in the key and once with the destination being put first in
 * the key. This is to enable an edge to be found in a Range scan when providing
 * only one end of the edge.
 */
public class AccumuloStore extends Store {
    public static final Set<StoreTrait> TRAITS =
            Collections.unmodifiableSet(Sets.newHashSet(
                    ORDERED,
                    VISIBILITY,
                    INGEST_AGGREGATION,
                    QUERY_AGGREGATION,
                    PRE_AGGREGATION_FILTERING,
                    POST_AGGREGATION_FILTERING,
                    POST_TRANSFORMATION_FILTERING,
                    TRANSFORMATION,
                    STORE_VALIDATION
            ));
    public static final String FAILED_TO_CREATE_AN_ACCUMULO_FROM_ELEMENT_OF_TYPE_WHEN_TRYING_TO_INSERT_ELEMENTS = "Failed to create an accumulo {} from element of type {} when trying to insert elements";
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloStore.class);
    private AccumuloKeyPackage keyPackage;
    private Connector connection = null;

    @Override
    public void initialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        preInitialise(graphId, schema, properties);
        TableUtils.ensureTableExists(this);
    }

    /**
     * Performs general initialisation without creating the table.
     *
     * @param graphId    The graph ID.
     * @param schema     The Gaffer Schema.
     * @param properties The Accumulo store properties.
     * @throws StoreException If the store could not be initialised.
     */
    public void preInitialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        setProperties(properties);

        final String deprecatedTableName = getProperties().getTable();
        if (null == graphId && null != deprecatedTableName) {
            // Deprecated
            super.initialise(deprecatedTableName, schema, getProperties());
        } else if (null != deprecatedTableName && !deprecatedTableName.equals(graphId)) {
            throw new IllegalArgumentException(
                    "The table in store.properties should no longer be used. " +
                            "Please use a graphId instead or for now just set the graphId to be the same value as the store.properties table.");
        } else {
            super.initialise(graphId, schema, getProperties());
        }
        final String keyPackageClass = getProperties().getKeyPackageClass();
        try {
            this.keyPackage = Class.forName(keyPackageClass).asSubclass(AccumuloKeyPackage.class).newInstance();
        } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new StoreException("Unable to construct an instance of key package: " + keyPackageClass, e);
        }
        this.keyPackage.setSchema(getSchema());
    }

    /**
     * Creates an Accumulo {@link org.apache.accumulo.core.client.Connector}
     * using the properties found in properties file associated with the
     * AccumuloStore.
     *
     * @return A new {@link Connector}.
     * @throws StoreException If there is a failure to connect to accumulo.
     */
    public Connector getConnection() throws StoreException {
        if (null == connection) {
            connection = TableUtils.getConnector(getProperties().getInstance(), getProperties().getZookeepers(),
                    getProperties().getUser(), getProperties().getPassword());
        }
        return connection;
    }

    public String getTableName() {
        return getGraphId();
    }

    /**
     * Updates a Hadoop {@link Configuration} with information needed to connect to the Accumulo store. It adds
     * iterators to apply the provided {@link View}. This method will be used by operations that run MapReduce
     * or Spark jobs against the Accumulo store.
     *
     * @param conf         A {@link Configuration} to be updated.
     * @param graphFilters The operation {@link GraphFilters} to be applied.
     * @param user         The {@link User} to be used.
     * @throws StoreException If there is a failure to connect to Accumulo or a problem setting the iterators.
     */
    public void updateConfiguration(final Configuration conf, final GraphFilters graphFilters, final User user) throws StoreException {
        try {
            final View view = graphFilters.getView();

            // Table name
            LOGGER.info("Updating configuration with table name of {}", getTableName());
            InputConfigurator.setInputTableName(AccumuloInputFormat.class,
                    conf,
                    getTableName());
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
            LOGGER.info("Updating configuration with authorizations of {}", authorisations);
            // Zookeeper
            addZookeeperToConfiguration(conf);
            // Add keypackage, schema and view to conf
            conf.set(ElementInputFormat.KEY_PACKAGE, getProperties().getKeyPackageClass());
            LOGGER.info("Updating configuration with key package of {}", getProperties().getKeyPackageClass());
            conf.set(ElementInputFormat.SCHEMA, new String(getSchema().toCompactJson(), CommonConstants.UTF_8));
            LOGGER.debug("Updating configuration with Schema of {}", getSchema());
            conf.set(ElementInputFormat.VIEW, new String(view.toCompactJson(), CommonConstants.UTF_8));
            LOGGER.debug("Updating configuration with View of {}", view);

            if (view.hasGroups()) {
                // Add the columns to fetch
                final Collection<org.apache.accumulo.core.util.Pair<Text, Text>> columnFamilyColumnQualifierPairs
                        = Stream
                        .concat(view.getEntityGroups().stream(), view.getEdgeGroups().stream())
                        .map(g -> new org.apache.accumulo.core.util.Pair<>(new Text(g), (Text) null))
                        .collect(Collectors.toSet());
                InputConfigurator.fetchColumns(AccumuloInputFormat.class, conf, columnFamilyColumnQualifierPairs);
                LOGGER.info("Updated configuration with column family/qualifiers of {}",
                        StringUtils.join(columnFamilyColumnQualifierPairs, ','));

                // Add iterators that depend on the view
                final IteratorSetting elementPreFilter = getKeyPackage()
                        .getIteratorFactory()
                        .getElementPreAggregationFilterIteratorSetting(view, this);
                if (null != elementPreFilter) {
                    InputConfigurator.addIterator(AccumuloInputFormat.class, conf, elementPreFilter);
                    LOGGER.info("Added pre-aggregation filter iterator of {}", elementPreFilter);
                }
                final IteratorSetting elementPostFilter = getKeyPackage()
                        .getIteratorFactory()
                        .getElementPostAggregationFilterIteratorSetting(view, this);
                if (null != elementPostFilter) {
                    InputConfigurator.addIterator(AccumuloInputFormat.class, conf, elementPostFilter);
                    LOGGER.info("Added post-aggregation filter iterator of {}", elementPostFilter);
                }
                final IteratorSetting edgeEntityDirFilter = getKeyPackage()
                        .getIteratorFactory()
                        .getEdgeEntityDirectionFilterIteratorSetting(graphFilters);
                if (null != edgeEntityDirFilter) {
                    InputConfigurator.addIterator(AccumuloInputFormat.class, conf, edgeEntityDirFilter);
                    LOGGER.info("Added edge direction filter iterator of {}", edgeEntityDirFilter);
                }
            }
        } catch (final AccumuloSecurityException | IteratorSettingException | UnsupportedEncodingException e) {
            throw new StoreException(e);
        }
    }

    @Override
    protected SchemaOptimiser createSchemaOptimiser() {
        return new SchemaOptimiser(new AccumuloSerialisationFactory());
    }

    @Override
    public void validateSchemas() {
        super.validateSchemas();
        validateConsistentVertex();
    }

    @Override
    protected void validateSchemaElementDefinition(final Entry<String, SchemaElementDefinition> schemaElementDefinitionEntry, final ValidationResult validationResult) {
        super.validateSchemaElementDefinition(schemaElementDefinitionEntry, validationResult);
        validateConsistentGroupByProperties(schemaElementDefinitionEntry, validationResult);
    }

    @Override
    protected Class<? extends ToBytesSerialiser> getRequiredParentSerialiserClass() {
        return ToBytesSerialiser.class;
    }

    protected void addUserToConfiguration(final Configuration conf) throws AccumuloSecurityException {
        LOGGER.info("Updating configuration with user of {}", getProperties().getUser());
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

    /**
     * Gets all {@link AccumuloProperties} related to the store.
     *
     * @return {@link AccumuloProperties}.
     */
    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "The properties should always be AccumuloProperties")
    @Override
    public AccumuloProperties getProperties() {
        return (AccumuloProperties) super.getProperties();
    }

    @Override
    protected Class<AccumuloProperties> getPropertiesClass() {
        return AccumuloProperties.class;
    }

    @Override
    protected void addAdditionalOperationHandlers() {
        addOperationHandler(AddElementsFromHdfs.class, new AddElementsFromHdfsHandler());
        addOperationHandler(GetElementsBetweenSets.class, new GetElementsBetweenSetsHandler());
        addOperationHandler(GetElementsWithinSet.class, new GetElementsWithinSetHandler());
        addOperationHandler(SplitStoreFromFile.class, new HdfsSplitStoreFromFileHandler());
        addOperationHandler(SplitStoreFromIterable.class, new SplitStoreFromIterableHandler());
        addOperationHandler(SplitStore.class, new SplitStoreHandler());
        addOperationHandler(SampleElementsForSplitPoints.class, new SampleElementsForSplitPointsHandler());
        addOperationHandler(SampleDataForSplitPoints.class, new SampleDataForSplitPointsHandler());
        addOperationHandler(ImportAccumuloKeyValueFiles.class, new ImportAccumuloKeyValueFilesHandler());

        if (null == getSchema().getVertexSerialiser() || getSchema().getVertexSerialiser().preservesObjectOrdering()) {
            addOperationHandler(SummariseGroupOverRanges.class, new SummariseGroupOverRangesHandler());
            addOperationHandler(GetElementsInRanges.class, new GetElementsInRangesHandler());
        } else {
            LOGGER.warn("Accumulo range scan operations will not be available on this store as the vertex serialiser does not preserve object ordering. Vertex serialiser: {}",
                    getSchema().getVertexSerialiser().getClass().getName());
        }
    }

    @Override
    protected OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> getGetElementsHandler() {
        return new GetElementsHandler();
    }

    @Override
    protected OutputOperationHandler<GetAllElements, CloseableIterable<? extends Element>> getGetAllElementsHandler() {
        return new GetAllElementsHandler();
    }

    @Override
    protected OutputOperationHandler<GetAdjacentIds, CloseableIterable<? extends EntityId>> getAdjacentIdsHandler() {
        return new GetAdjacentIdsHandler();
    }

    @Override
    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        return new AddElementsHandler();
    }

    @Override
    public Set<StoreTrait> getTraits() {
        return TRAITS;
    }

    /**
     * Method to add {@link Element}s into Accumulo.
     *
     * @param elements The elements to be added.
     * @throws StoreException If there is a failure to insert the elements into a table.
     */
    public void addElements(final Iterable<? extends Element> elements) throws StoreException {
        insertGraphElements(elements);
    }

    protected void insertGraphElements(final Iterable<? extends Element> elements) throws StoreException {
        // Create BatchWriter
        final BatchWriter writer = TableUtils.createBatchWriter(this);
        // Loop through elements, convert to mutations, and add to
        // BatchWriter.as
        // The BatchWriter takes care of batching them up, sending them without
        // too high a latency, etc.
        if (null != elements) {
            for (final Element element : elements) {

                final Pair<Key, Key> keys;
                try {
                    keys = keyPackage.getKeyConverter().getKeysFromElement(element);
                } catch (final AccumuloElementConversionException e) {
                    LOGGER.error(FAILED_TO_CREATE_AN_ACCUMULO_FROM_ELEMENT_OF_TYPE_WHEN_TRYING_TO_INSERT_ELEMENTS, "key", element.getGroup());
                    continue;
                }
                final Value value;
                try {
                    value = keyPackage.getKeyConverter().getValueFromElement(element);
                } catch (final AccumuloElementConversionException e) {
                    LOGGER.error(FAILED_TO_CREATE_AN_ACCUMULO_FROM_ELEMENT_OF_TYPE_WHEN_TRYING_TO_INSERT_ELEMENTS, "value", element.getGroup());
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
                if (null != keys.getSecond()) {
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
     * Gets the {@link uk.gov.gchq.gaffer.accumulostore.key.AccumuloKeyPackage} in use by
     * this AccumuloStore.
     *
     * @return {@link uk.gov.gchq.gaffer.accumulostore.key.AccumuloKeyPackage}.
     */
    public AccumuloKeyPackage getKeyPackage() {
        return keyPackage;
    }

    /**
     * Gets the TabletServers.
     *
     * @return A list of Strings of TabletServers.
     * @throws StoreException If failure.
     */
    public List<String> getTabletServers() throws StoreException {
        return getConnection().instanceOperations().getTabletServers();
    }

    private void addHdfsOperationHandler(final Class<? extends Operation> opClass, final OperationHandler handler) {
        try {
            addOperationHandler(opClass, handler);
        } catch (final NoClassDefFoundError e) {
            LOGGER.warn("Unable to added handler for {} due to missing classes on the classpath", opClass.getSimpleName(), e);
        }
    }
}
