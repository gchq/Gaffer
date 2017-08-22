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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.inputformat.ElementInputFormat;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloKeyPackage;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfAllElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.directrdd.RFileReaderRDD;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.directrdd.Utils;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import static uk.gov.gchq.gaffer.accumulostore.inputformat.ElementInputFormat.KEY_PACKAGE;
import static uk.gov.gchq.gaffer.accumulostore.inputformat.ElementInputFormat.SCHEMA;
import static uk.gov.gchq.gaffer.spark.operation.dataframe.ClassTagConstants.ELEMENT_CLASS_TAG;

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
        final String useRFileReaderRDD = operation.getOption(USE_RFILE_READER_RDD);
        if (null != useRFileReaderRDD && useRFileReaderRDD.equalsIgnoreCase("true")) {
            return doOperationUsingRFileReaderRDD(operation, context, accumuloStore);
        } else {
            return doOperationUsingElementInputFormat(operation, context, accumuloStore);
        }
    }

    private RDD<Element> doOperationUsingElementInputFormat(final GetRDDOfAllElements operation,
                                                            final Context context,
                                                            final AccumuloStore accumuloStore)
            throws OperationException {
        final SparkContext sparkContext = operation.getSparkSession().sparkContext();
        final Configuration conf = getConfiguration(operation);
        addIterators(accumuloStore, conf, context.getUser(), operation);
        final RDD<Tuple2<Element, NullWritable>> pairRDD = sparkContext.newAPIHadoopRDD(conf,
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
        addIterators(accumuloStore, conf, context.getUser(), operation);
        try {
            // Add view to conf so that any transformations can be applied
            conf.set(AbstractGetRDDHandler.VIEW, new String(operation.getView().toCompactJson(), CommonConstants.UTF_8));
            final byte[] serialisedConf = Utils.serialiseConfiguration(conf);
            final RDD<Map.Entry<Key, Value>> rdd = new RFileReaderRDD(
                    operation.getSparkSession().sparkContext().getConf(),
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

    public static class EntryIteratorToElementIterator
            extends AbstractFunction1<Iterator<Map.Entry<Key, Value>>, Iterator<Element>> implements Serializable {
        private byte[] serialisedConf;

        public EntryIteratorToElementIterator(final byte[] serialisedConf) {
            this.serialisedConf = serialisedConf;
        }

        @Override
        public Iterator<Element> apply(final Iterator<Map.Entry<Key, Value>> entryIterator) {
            final EntryToElement entryToElement = new EntryToElement(serialisedConf);
            return entryIterator.map(entryToElement);
        }
    }

    public static class EntryToElement extends AbstractFunction1<Map.Entry<Key, Value>, Element> implements Serializable {
        private transient AccumuloElementConverter converter;
        private transient View view;

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
            if (viewDef != null) {
                final ElementTransformer transformer = viewDef.getTransformer();
                if (transformer != null) {
                    transformer.apply(element);
                }
            }
            return element;
        }
    }
}
