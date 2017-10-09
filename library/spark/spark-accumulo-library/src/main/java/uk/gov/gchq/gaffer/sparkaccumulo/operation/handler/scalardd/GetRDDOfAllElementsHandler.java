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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.scalardd;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.inputformat.ElementInputFormat;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloKeyPackage;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewUtil;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.SparkContextUtil;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfAllElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.rfilereaderrdd.RFileReaderRDD;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.rfilereaderrdd.Utils;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import static uk.gov.gchq.gaffer.accumulostore.inputformat.ElementInputFormat.KEY_PACKAGE;
import static uk.gov.gchq.gaffer.accumulostore.inputformat.ElementInputFormat.SCHEMA;
import static uk.gov.gchq.gaffer.spark.operation.dataframe.ClassTagConstants.ELEMENT_CLASS_TAG;

/**
 * A handler for the {@link GetRDDOfAllElements} operation.
 * <p>
 * <p>If the {@code gaffer.accumulo.spark.directrdd.use_rfile_reader} option is set to {@code true} then the
 * RDD will be produced by directly reading the RFiles in the Accumulo table, rather than using
 * {@link ElementInputFormat} to get data via the tablet servers. In order to read the RFiles directly, the user must
 * have read access to the files. Also note that any data that has not been minor compacted will not be read. Reading
 * the Rfiles directly can increase the performance.
 * <p>
 * <p>If the {@code gaffer.accumulo.spark.directrdd.use_rfile_reader} option is not set then the standard approach
 * of obtaining data via the tablet servers is used.
 */
public class GetRDDOfAllElementsHandler extends AbstractGetRDDHandler<GetRDDOfAllElements, RDD<Element>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetRDDOfAllElementsHandler.class);

    @Override
    public RDD<Element> doOperation(final GetRDDOfAllElements operation,
                                    final Context context,
                                    final Store store)
            throws OperationException {
        return doOperation(operation, context, (AccumuloStore) store);
    }

    private RDD<Element> doOperation(final GetRDDOfAllElements operation,
                                     final Context context,
                                     final AccumuloStore accumuloStore)
            throws OperationException {
        SparkSession sparkSession = SparkContextUtil.getSparkSession(context, accumuloStore.getProperties());
        if (sparkSession == null) {
            throw new OperationException("This operation requires an active SparkSession.");
        }
        sparkSession.sparkContext().hadoopConfiguration().addResource(getConfiguration(operation));
        final String useRFileReaderRDD = operation.getOption(USE_RFILE_READER_RDD);
        if (Boolean.parseBoolean(useRFileReaderRDD)) {
            return doOperationUsingRFileReaderRDD(operation, context, accumuloStore);
        } else {
            return doOperationUsingElementInputFormat(operation, context, accumuloStore);
        }
    }

    private RDD<Element> doOperationUsingElementInputFormat(final GetRDDOfAllElements operation,
                                                            final Context context,
                                                            final AccumuloStore accumuloStore)
            throws OperationException {
        final Configuration conf = getConfiguration(operation);
        addIterators(accumuloStore, conf, context.getUser(), operation);
        final RDD<Tuple2<Element, NullWritable>> pairRDD = SparkContextUtil.getSparkSession(context, accumuloStore.getProperties()).sparkContext().newAPIHadoopRDD(conf,
                ElementInputFormat.class,
                Element.class,
                NullWritable.class);
        return pairRDD.map(new FirstElement(), ELEMENT_CLASS_TAG);
    }

    private RDD<Element> doOperationUsingRFileReaderRDD(final GetRDDOfAllElements operation,
                                                        final Context context,
                                                        final AccumuloStore accumuloStore)
            throws OperationException {
        final Configuration conf = getConfiguration(operation);
        // Need to add validation iterator manually (it's not added by the addIterators method as normally the iterator
        // is present on the table and therefore applied to all scans - here we're bypassing the normal table access
        // method so it needs to be applied manually)
        addValidationIterator(accumuloStore, conf);
        // Need to add aggregation iterator manually for the same reasons as above
        try {
            addAggregationIterator(accumuloStore, conf);
        } catch (final IteratorSettingException e) {
            throw new OperationException("IteratorSettingException adding aggregation iterator", e);
        }
        // Add other iterators
        addIterators(accumuloStore, conf, context.getUser(), operation);
        try {
            // Add view to conf so that any transformations can be applied
            conf.set(AbstractGetRDDHandler.VIEW, new String(operation.getView().toCompactJson(), CommonConstants.UTF_8));
            final byte[] serialisedConf = Utils.serialiseConfiguration(conf);
            final RDD<Map.Entry<Key, Value>> rdd = new RFileReaderRDD(
                    SparkContextUtil.getSparkSession(context, accumuloStore.getProperties()).sparkContext(),
                    accumuloStore.getProperties().getInstance(),
                    accumuloStore.getProperties().getZookeepers(),
                    accumuloStore.getProperties().getUser(),
                    accumuloStore.getProperties().getPassword(),
                    accumuloStore.getTableName(),
                    context.getUser().getDataAuths(),
                    serialisedConf);
            return rdd.mapPartitions(new EntryIteratorToElementIterator(serialisedConf), true, ELEMENT_CLASS_TAG);
        } catch (final IOException e) {
            throw new OperationException("IOException serialising configuration", e);
        }
    }

    private void addValidationIterator(final AccumuloStore accumuloStore, final Configuration conf) {
        if (accumuloStore.getProperties().getEnableValidatorIterator()) {
            final IteratorSetting itrSetting = accumuloStore
                    .getKeyPackage().getIteratorFactory().getValidatorIteratorSetting(accumuloStore);
            if (null == itrSetting) {
                LOGGER.info("Not adding validation iterator as no validation functions are defined in the schema");
            } else {
                LOGGER.info("Adding validation iterator");
                InputConfigurator.addIterator(AccumuloInputFormat.class, conf, itrSetting);
            }
        }
    }

    private void addAggregationIterator(final AccumuloStore accumuloStore, final Configuration conf)
            throws IteratorSettingException {
        if (accumuloStore.getSchema().isAggregationEnabled()) {
            // Add aggregator iterator to table for all scopes
            LOGGER.info("Adding aggregator iterator");
            final IteratorSetting itrSetting = accumuloStore
                    .getKeyPackage().getIteratorFactory().getAggregatorIteratorSetting(accumuloStore);
            InputConfigurator.addIterator(AccumuloInputFormat.class, conf, itrSetting);
        } else {
            LOGGER.info("Not adding aggregator iterator as aggregation is not enabled");
        }
    }

    public static class EntryIteratorToElementIterator
            extends AbstractFunction1<Iterator<Map.Entry<Key, Value>>, Iterator<Element>> implements Serializable {
        private byte[] serialisedConf;

        public EntryIteratorToElementIterator(final byte[] serialisedConf) {
            this.serialisedConf = serialisedConf;
        }

        @Override
        public Iterator<Element> apply(final Iterator<Map.Entry<Key, Value>> entryIterator) {
            final EntryToElement entryToElement = new EntryToElement(serialisedConf);
            return entryIterator
                    .map(entryToElement)
                    .filter(new FilterOutNull());
        }
    }

    public static class FilterOutNull extends AbstractFunction1<Element, Object> implements Serializable {
        @Override
        public Object apply(final Element element) {
            if (null != element) {
                return true;
            }
            return false;
        }
    }

    public static class EntryToElement extends AbstractFunction1<Map.Entry<Key, Value>, Element> {
        private AccumuloElementConverter converter;
        private View view;

        public EntryToElement(final byte[] serialisedConf) {
            try {
                final Configuration conf = Utils.deserialiseConfiguration(serialisedConf);
                final String keyPackageClass = conf.get(KEY_PACKAGE);
                final Schema schema = Schema.fromJson(conf.get(SCHEMA).getBytes(CommonConstants.UTF_8));
                final AccumuloKeyPackage keyPackage = Class
                        .forName(keyPackageClass)
                        .asSubclass(AccumuloKeyPackage.class)
                        .newInstance();
                keyPackage.setSchema(schema);
                converter = keyPackage.getKeyConverter();
                LOGGER.info("Initialised EntryToElement with AccumuloElementConverter of {}", converter.getClass().getName());
                view = View.fromJson(conf.get(AbstractGetRDDHandler.VIEW).getBytes(CommonConstants.UTF_8));
                LOGGER.info("Initialised EntryToElement with View of {}", view.toString());
            } catch (final InstantiationException | IllegalAccessException | ClassNotFoundException | IOException e) {
                throw new RuntimeException("Exception creating AccumuloKeyPackage from Configuration", e);
            }
        }

        @Override
        public Element apply(final Map.Entry<Key, Value> entry) {
            final Element element = converter.getFullElement(entry.getKey(), entry.getValue(), false);
            final ViewElementDefinition viewDef = view.getElement(element.getGroup());
            if (null != viewDef) {
                final ElementTransformer transformer = viewDef.getTransformer();
                if (null != transformer) {
                    transformer.apply(element);
                }
                if (ElementInputFormat.doPostFilter(element, view)) {
                    ViewUtil.removeProperties(view, element);
                    return element;
                } else {
                    return null;
                }

            }
            return element;
        }
    }
}
