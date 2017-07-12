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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.javardd;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.inputformat.ElementInputFormat;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.operation.javardd.GetJavaRDDOfElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.directrdd.RFileReaderRDD;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.directrdd.Utils;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.io.IOException;
import java.util.Map;

import static uk.gov.gchq.gaffer.spark.operation.dataframe.ClassTagConstants.ELEMENT_CLASS_TAG;

public class GetJavaRDDOfElementsHandler extends AbstractGetRDDHandler<GetJavaRDDOfElements, JavaRDD<Element>> {
    public static final String USE_RFILE_READER_RDD = "gaffer.accumulo.spark.rfilereader";

    @Override
    public JavaRDD<Element> doOperation(final GetJavaRDDOfElements operation,
                                        final Context context,
                                        final Store store) throws OperationException {
        return doOperation(operation, context, (AccumuloStore) store);
    }

    private JavaRDD<Element> doOperation(final GetJavaRDDOfElements operation,
                                         final Context context,
                                         final AccumuloStore accumuloStore) throws OperationException {
        if (null != operation.getOption(USE_RFILE_READER_RDD)) {
            return doOperationUsingRFileReaderRDD(operation, accumuloStore);
        }
        final JavaSparkContext sparkContext = operation.getJavaSparkContext();
        final Configuration conf = getConfiguration(operation);
        // Use batch scan option when performing seeded operation
        InputConfigurator.setBatchScan(AccumuloInputFormat.class, conf, true);
        addIterators(accumuloStore, conf, context.getUser(), operation);
        addRanges(accumuloStore, conf, operation);
        final JavaPairRDD<Element, NullWritable> pairRDD = sparkContext.newAPIHadoopRDD(conf,
                ElementInputFormat.class,
                Element.class,
                NullWritable.class);
        final JavaRDD<Element> rdd = pairRDD.map(new FirstElement());
        return rdd;
    }

    private JavaRDD<Element> doOperationUsingRFileReaderRDD(final GetJavaRDDOfElements operation,
                                                            final AccumuloStore accumuloStore) throws OperationException {
        try {
            final RDD<Map.Entry<Key, Value>> rdd = new RFileReaderRDD(operation.getJavaSparkContext().getConf(),
                    accumuloStore.getProperties().getInstance(),
                    accumuloStore.getProperties().getZookeepers(),
                    accumuloStore.getProperties().getUser(),
                    accumuloStore.getProperties().getPassword(),
                    accumuloStore.getProperties().getTable(),
                    Utils.serialiseConfiguration(getConfiguration(operation)));
            final RDD<Element> elementRDD = rdd.mapPartitions(iterator -> {
                final AccumuloElementConverter converter = accumuloStore.getKeyPackage().getKeyConverter();
                return iterator.map(entry -> converter.getFullElement(entry.getKey(), entry.getValue()));
            }, true, ELEMENT_CLASS_TAG);
            return elementRDD.toJavaRDD();
        } catch (final IOException e) {
            throw new OperationException("IOException creating RFileReaderRDD", e);
        }
    }

    static class FirstElement implements Function<Tuple2<Element, NullWritable>, Element> {

        private static final long serialVersionUID = -4695668644733530293L;

        @Override
        public Element call(final Tuple2<Element, NullWritable> tuple) throws Exception {
            return tuple._1();
        }
    }
}
