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

package gaffer.spark;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gaffer.accumulostore.utils.IteratorSettingBuilder;
import gaffer.accumulostore.utils.Pair;
import gaffer.commonutil.CommonConstants;
import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.key.AccumuloKeyPackage;
import gaffer.accumulostore.key.exception.IteratorSettingException;
import gaffer.accumulostore.key.exception.RangeFactoryException;
import gaffer.accumulostore.operation.impl.GetEdgesBetweenSets;
import gaffer.accumulostore.operation.impl.GetElementsInRanges;
import gaffer.data.element.Element;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.exception.SchemaException;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.function.simple.filter.IsLessThan;
import gaffer.function.simple.filter.IsMoreThan;
import gaffer.operation.data.EdgeSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.store.StoreException;

/**
 * A custom configuration class that sets the configuration for MapReduce Jobs
 * on an Accumulo Graph.
 */
public class GraphConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphConfig.class);

    // Connection to Accumulo
    protected Connector connector;
    protected Authorizations authorizations;

    // View on the graph
    protected boolean summarise;
    protected boolean returnEntities;
    protected boolean returnEdges;
    protected ViewElementDefinition viewDef;
    protected Set<String> entityGroups;
    protected Set<String> edgeGroups;

    protected AccumuloStore store;
    protected Instance instance;
    protected AccumuloKeyPackage keyPackage;
    protected IteratorSetting keySetting;

    public GraphConfig(final AccumuloStore store) {
        this.store = store;
        keyPackage = store.getKeyPackage();
        try {
            instance = new ZooKeeperInstance(store.getProperties().getInstanceName(),
                    store.getProperties().getZookeepers());
            connector = instance.getConnector(store.getProperties().getUserName(),
                    new PasswordToken(store.getProperties().getPassword()));
            authorizations = connector.securityOperations().getUserAuthorizations(connector.whoami());
        } catch (AccumuloException | AccumuloSecurityException e) {
            LOGGER.error(e.getMessage());
        }

        summarise = true;
        returnEntities = true;
        returnEdges = true;
        entityGroups = new HashSet<>(store.getSchema().getEntityGroups());
        edgeGroups = new HashSet<>(store.getSchema().getEdgeGroups());
    }

    /**
     * Updates the provided {@link Configuration} with options that specify the
     * view on the graph (i.e. the authorizations, a range of property values or
     * vertices, whether we want to summarise, and whether we want entities only
     * or edges only or entities and edges). This allows a MapReduce job that uses
     * the {@link Configuration} to receive data that is subject to the same view
     * as is on the graph.
     *
     * Note that this method will not produce the same edge twice.
     *
     * @param conf The {@link Configuration} for a MapReduce job.
     */
    public void setConfiguration(final Configuration conf) {
        try {
            addAccumuloInfoToConfiguration(conf);
            addRollUpAndAuthsToConfiguration(conf);
            addViewsToConfiguration(conf);
            addKeyPackageAndSchemaToConfiguration(conf);
        } catch (AccumuloSecurityException | StoreException | AccumuloException e) {
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * Updates the provided {@link Configuration} with options that specify the
     * view on the graph (i.e. the authorizations, a range of property values or
     * vertices, whether we want to summarise, and whether we want entities only
     * or edges only or entities and edges), and sets it to only return data that
     * involves the given {@link Object} vertices. This allows a MapReduce job that
     * uses the {@link Configuration} to receive data that only involves the given
     * {@link Object}s and is subject to the same view as is on the graph.
     *
     * Note that this method may cause the same edge to be returned
     * twice if both ends are in the provided set.
     *
     * @param conf The {@link Configuration} for a MapReduce job.
     * @param vertices The vertices to be queried for.
     * @throws RangeFactoryException When an error occurs creating a range.
     */
    public void setConfiguration(final Configuration conf, final Collection<Object> vertices) throws RangeFactoryException {
        // NB: Do not need to add the Entity or Edge only iterator here, as the
        // ranges that are
        // created take care of that.

        try {
            addAccumuloInfoToConfiguration(conf);
            addRollUpAndAuthsToConfiguration(conf);
            addViewsToConfiguration(conf);
            addKeyPackageAndSchemaToConfiguration(conf);
            Set<Range> ranges = new HashSet<>();

            for (Object vertex : vertices) {
                EntitySeed seed = new EntitySeed(vertex);
                ranges.addAll(keyPackage.getRangeFactory().getRange(seed,
                        new GetRelatedElements<EntitySeed, Element>(Collections.singletonList(seed))));
            }
            InputConfigurator.setRanges(AccumuloInputFormat.class, conf, ranges);
        } catch (AccumuloSecurityException | StoreException | AccumuloException e) {
            LOGGER.error(e.getMessage());
        }

    }

    /**
     * Updates the provided {@link Configuration} with options that specify the
     * view on the graph (i.e. the authorizations, a range of property values or
     * vertices, whether we want to summarise, and whether we want entities only
     * or edges only or entities and edges), and sets it to only return data that
     * is in the given range made up of a {@link Pair} of vertices. This allows a
     * MapReduce job that uses the {@link Configuration} to only receive data that
     * involves the given vertex range and is subject to the same view as is on
     * the graph.
     *
     * Note that this method may cause the same edge to be returned
     * twice if both ends are in the provided range.
     *
     * @param conf The {@link Configuration} for a MapReduce job.
     * @param vertexRange The range of vertices to be queried for.
     * @throws RangeFactoryException When an error occurs creating a range.
     */
    public void setConfigurationFromRanges(final Configuration conf, final Pair<Object> vertexRange) throws RangeFactoryException {
        setConfigurationFromRanges(conf, Collections.singleton(vertexRange));
    }

    /**
     * Updates the provided {@link Configuration} with options that specify the
     * view on the graph (i.e. the authorizations, a range of property values or
     * vertices, whether we want to summarise, and whether we want entities only
     * or edges only or entities and edges), and sets it to only return data that
     * is in the given range made up of {@link Pair}s of vertices. This allows a
     * MapReduce job that uses the {@link Configuration} to only receive data that
     * involves the given ranges and is subject to the same view as is on the graph.
     *
     * Note that this method may cause the same edge to be returned
     * twice if both ends are in the provided ranges.
     *
     * @param conf The {@link Configuration} for a MapReduce job.
     * @param vertexRanges The ranges of vertices to be queried for.
     * @throws RangeFactoryException When an error occurs creating a range.
     */
    public void setConfigurationFromRanges(final Configuration conf, final Collection<Pair<Object>> vertexRanges) throws RangeFactoryException {

        try {
            addAccumuloInfoToConfiguration(conf);
            addRollUpAndAuthsToConfiguration(conf);
            addViewsToConfiguration(conf);
            addKeyPackageAndSchemaToConfiguration(conf);
            Set<Range> ranges = new HashSet<>();
            for (Pair<Object> vertexRange : vertexRanges) {
                Pair<EntitySeed> seedRange = new Pair<>(new EntitySeed(vertexRange.getFirst()),
                        new EntitySeed(vertexRange.getSecond()));
                final ArrayList<Range> ran = new ArrayList<>();

                ran.addAll(keyPackage.getRangeFactory().getRange(seedRange.getFirst(),
                        new GetElementsInRanges<Pair<EntitySeed>, Element>(Collections.singletonList(seedRange))));
                ran.addAll(keyPackage.getRangeFactory().getRange(seedRange.getSecond(),
                        new GetElementsInRanges<Pair<EntitySeed>, Element>(Collections.singletonList(seedRange))));

                Range min = null;
                Range max = null;
                for (final Range range : ran) {
                    if (min == null) {
                        min = range;
                        max = range;
                    }
                    if (range.compareTo(min) < 0) {
                        min = range;
                    } else if (range.compareTo(max) > 0) {
                        max = range;
                    }
                }
                Range r = new Range(min.getStartKey(), max.getEndKey());
                ranges.add(r);
            }
            InputConfigurator.setRanges(AccumuloInputFormat.class, conf, ranges);
        } catch (AccumuloSecurityException | StoreException | AccumuloException e) {
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * Updates the provided {@link Configuration} with options that specify the
     * view on the graph (i.e. the authorizations, a range of property values or
     * vertices, whether we want to summarise, and sets it to only return edges
     * that involve the given pair of {@link Object} vertices. This allows a MapReduce
     * job that uses the {@link Configuration} to only receive edges that involves the
     * given pair of vertices and is subject to the same view as is on the graph.
     *
     * @param conf The {@link Configuration} for a MapReduce job.
     * @param vertexPair The pair of vertices to be queried for.
     * @throws RangeFactoryException When an error occurs creating a range.
     */
    public void setConfigurationFromPairs(final Configuration conf, final Pair<Object> vertexPair) throws RangeFactoryException {
        setConfigurationFromPairs(conf, Collections.singleton(vertexPair));
    }

    /**
     * Updates the provided {@link Configuration} with options that specify the
     * view on the graph (i.e. the authorizations, a range of property values or
     * vertices, whether we want to summarise, and sets it to only return edges
     * that involve the given pairs of {@link Object} vertices. This allows a
     * MapReduce job that uses the {@link Configuration} to only receive edges
     * that involves the given pairs of vertices and is subject to the same view
     * as is on the graph.
     *
     * @param conf The {@link Configuration} for a MapReduce job.
     * @param vertexPairs The pairs of vertices to be queried for.
     * @throws RangeFactoryException When an error occurs creating a range.
     */
    public void setConfigurationFromPairs(final Configuration conf, final Collection<Pair<Object>> vertexPairs) throws RangeFactoryException {
        // NB: Do not need to add the Entity or Edge only iterator here, as the
        // ranges that are
        // created take care of that.
        try {
            addAccumuloInfoToConfiguration(conf);
            addRollUpAndAuthsToConfiguration(conf);
            addViewsToConfiguration(conf);
            addKeyPackageAndSchemaToConfiguration(conf);
            Set<Range> ranges = new HashSet<>();
            for (Pair<Object> vertexPair : vertexPairs) {
                EdgeSeed seed1 = new EdgeSeed(vertexPair.getFirst(), vertexPair.getSecond(), true);
                EdgeSeed seed2 = new EdgeSeed(vertexPair.getSecond(), vertexPair.getFirst(), true);

                ranges.addAll(
                        keyPackage.getRangeFactory().getRange(seed1,
                                new GetEdgesBetweenSets(
                                        Collections.singletonList(new EntitySeed(vertexPair.getFirst())),
                                        Collections.singletonList(new EntitySeed(vertexPair.getSecond())))));
                ranges.addAll(
                        keyPackage.getRangeFactory().getRange(seed2,
                                new GetEdgesBetweenSets(
                                        Collections.singletonList(new EntitySeed(vertexPair.getFirst())),
                                        Collections.singletonList(new EntitySeed(vertexPair.getSecond())))));

            }
            InputConfigurator.setRanges(AccumuloInputFormat.class, conf, ranges);
        } catch (AccumuloSecurityException | StoreException | AccumuloException e) {
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * Sets the Accumulo information on the provided {@link Configuration}, i.e.
     * the table name, the connector, the authorizations, and the Zookeepers.
     *
     * @param conf The {@link Configuration} for a MapReduce job.
     * @throws AccumuloSecurityException If there is a failure to connect to accumulo.
     * @throws StoreException If there is a failure to connect to accumulo.
     * @throws AccumuloException If there is a failure to connect to accumulo.
     */
    protected void addAccumuloInfoToConfiguration(final Configuration conf)
            throws AccumuloSecurityException, StoreException, AccumuloException {
        // Table
        InputConfigurator.setInputTableName(AccumuloInputFormat.class, conf, store.getProperties().getTable());
        // Connector
        InputConfigurator.setConnectorInfo(AccumuloInputFormat.class, conf, store.getProperties().getUserName(),
                new PasswordToken(store.getProperties().getPassword()));
        // Authorizations
        InputConfigurator.setScanAuthorizations(AccumuloInputFormat.class, conf, authorizations);

        // Zookeeper
        InputConfigurator.setZooKeeperInstance(AccumuloInputFormat.class, conf,
                new ClientConfiguration().withInstance(store.getProperties().getInstanceName())
                .withZkHosts(store.getProperties().getZookeepers()));
    }

    /**
     * Sets the key package class and the schema on the {@link Configuration} so that the
     * store's relevant element converter can be initialised and used by the input format.
     *
     * @param conf The {@link Configuration} for a MapReduce job.
     */
    protected void addKeyPackageAndSchemaToConfiguration(final Configuration conf) {
        conf.set(ElementInputFormat.KEY_PACKAGE, store.getProperties().getKeyPackageClass());
        try {
            conf.set(ElementInputFormat.SCHEMA, new String(store.getSchema().toJson(true), CommonConstants.UTF_8));
        } catch (UnsupportedEncodingException | SchemaException e) {
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * Configures the pre roll-up filtering iterator to run with the provided
     * views.
     *
     * @param conf The {@link Configuration} for a MapReduce job.
     */
    protected void addViewsToConfiguration(final Configuration conf) {

        View.Builder builder = new View.Builder();
        ArrayList<View> views = new ArrayList<>();
        if (returnEntities) {
            builder.entities(entityGroups);
            if (viewDef != null) {
                for (String s : entityGroups) {
                    views.add(new View.Builder().entity(s, viewDef).build());
                }
            }
        }
        if (returnEdges) {
            builder.edges(edgeGroups);
            if (viewDef != null) {
                for (String s : edgeGroups) {
                    views.add(new View.Builder().edge(s, viewDef).build());
                }
            }
        }

        try {
            keySetting = keyPackage.getIteratorFactory().getElementFilterIteratorSetting(builder.build(), store);
        } catch (IteratorSettingException e) {
            e.printStackTrace();
        }

        if (viewDef == null) {
            InputConfigurator.addIterator(AccumuloInputFormat.class, conf, keySetting);
        } else {
            IteratorSettingBuilder build = new IteratorSettingBuilder(keySetting);
            for (View v : views) {
                build.view(v);
            }
            InputConfigurator.addIterator(AccumuloInputFormat.class, conf, build.build());
        }

        entityGroups.clear();
        entityGroups.addAll(store.getSchema().getEntityGroups());
        edgeGroups.clear();
        edgeGroups.addAll(store.getSchema().getEdgeGroups());
        viewDef = null;
    }

    /**
     * Sets the roll-up on the {@link Configuration}.
     *
     * @param conf The {@link Configuration} for a MapReduce job.
     */
    protected void addRollUpAndAuthsToConfiguration(final Configuration conf) {
        if (summarise) {
            try {
                InputConfigurator.addIterator(AccumuloInputFormat.class, conf,
                        keyPackage.getIteratorFactory().getQueryTimeAggregatorIteratorSetting(store));
            } catch (IteratorSettingException e) {
                LOGGER.error(e.getMessage());
            }
        }
    }

    /**
     * Allows the user to specify whether they want the results of queries to be
     * rolled up as a summary.
     *
     * @param rollUp Specifies rollUp.
     */
    public void summarise(final boolean rollUp) {
        this.summarise = rollUp;
    }

    /**
     * Allows the user to specify that they only want entities to be
     * returned by queries. This overrides any previous calls to
     * {@link #setReturnEdgesOnly} or {@link #setReturnEntitiesAndEdges}.
     */
    public void setReturnEntitiesOnly() {
        this.returnEntities = true;
        this.returnEdges = false;
    }

    /**
     * Allows the user to specify that they only want edges to be
     * returned by queries. This overrides any previous calls to
     * {@link #setReturnEntitiesOnly} or {@link #setReturnEntitiesAndEdges}.
     */
    public void setReturnEdgesOnly() {
        this.returnEntities = false;
        this.returnEdges = true;
    }

    /**
     * Allows the user to specify that they want both entities and
     * edges to be returned by queries. This overrides any previous
     * calls to {@link #setReturnEntitiesOnly} or {@link #setReturnEdgesOnly}.
     */
    public void setReturnEntitiesAndEdges() {
        this.returnEntities = true;
        this.returnEdges = true;
    }

    /**
     * Creates a {@link ViewElementDefinition} based on {@link Element}s whose
     * {@link Comparable} value is at or after the first value and at or before
     * the second value. The values relate to a given property {@link String}.
     *
     * @param prop
     *            The property relating to the range of values.
     * @param start
     *            The start value of the range to be queried.
     * @param end
     *            The end value of the range to be queried.
     */
    public void setRange(final String prop, final Comparable start, final Comparable end) {
        viewDef = new ViewElementDefinition.Builder().filter(new ElementFilter.Builder().select(prop)
                .execute(new IsMoreThan(start)).select(prop).execute(new IsLessThan(end)).build()).build();
    }

    /**
     * Creates a {@link ViewElementDefinition} based on {@link Element}s whose
     * {@link Comparable} value is at or before the value supplied. The value
     * relates to a given property {@link String}.
     *
     * @param prop
     *            The property relating to the range of values.
     * @param end
     *            The start value of the range to be queried.
     */
    public void setBefore(final String prop, final Comparable end) {
        viewDef = new ViewElementDefinition.Builder()
                .filter(new ElementFilter.Builder().select(prop).execute(new IsLessThan(end)).build()).build();
    }

    /**
     * Creates a {@link ViewElementDefinition} based on {@link Element}s whose
     * {@link Comparable} value is at or after the value supplied. The value
     * relates to a given property {@link String}.
     *
     * @param prop
     *            The property relating to the range of values.
     * @param start
     *            The start value of the range to be queried.
     */
    public void setAfter(final String prop, final Comparable start) {
        viewDef = new ViewElementDefinition.Builder()

                .filter(new ElementFilter.Builder().select(prop).execute(new IsMoreThan(start)).build()).build();
    }

    /**
     * Sets the entity and edge groups to only contain groups
     * that are in the given {@link Set}.
     *
     * @param groups
     *            The groups to be included.
     */
    public void setGroupTypes(final Set<String> groups) {
        entityGroups.clear();
        edgeGroups.clear();
        for (String group : groups) {
            if (store.getSchema().getEntityGroups().contains(group)) {
                entityGroups.add(group);
            }
            if (store.getSchema().getEdgeGroups().contains(group)) {
                edgeGroups.add(group);
            }

        }
    }
}
