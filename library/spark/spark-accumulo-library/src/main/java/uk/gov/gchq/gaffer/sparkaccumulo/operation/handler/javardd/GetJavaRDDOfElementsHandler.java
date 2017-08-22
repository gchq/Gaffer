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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.inputformat.ElementInputFormat;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.operation.javardd.GetJavaRDDOfElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

public class GetJavaRDDOfElementsHandler extends AbstractGetRDDHandler<GetJavaRDDOfElements, JavaRDD<Element>> {
//    public static final String USE_RFILE_READER_RDD = "gaffer.accumulo.spark.rfilereader";

    @Override
    public JavaRDD<Element> doOperation(final GetJavaRDDOfElements operation,
                                        final Context context,
                                        final Store store) throws OperationException {
        return doOperation(operation, context, (AccumuloStore) store);
    }

    private JavaRDD<Element> doOperation(final GetJavaRDDOfElements operation,
                                         final Context context,
                                         final AccumuloStore accumuloStore) throws OperationException {
//        if (null != operation.getOption(USE_RFILE_READER_RDD)) {
//            return doOperationUsingRFileReaderRDD(operation, accumuloStore);
//        }
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

//    private JavaRDD<Element> doOperationUsingRFileReaderRDD(final GetJavaRDDOfElements operation,
//                                                            final AccumuloStore accumuloStore) throws OperationException {
//        try {
//            final RDD<Map.Entry<Key, Value>> rdd = new RFileReaderRDD(
//                    operation.getJavaSparkContext().getConf(),
//                    accumuloStore.getProperties().getInstance(),
//                    accumuloStore.getProperties().getZookeepers(),
//                    accumuloStore.getProperties().getUser(),
//                    accumuloStore.getProperties().getPassword(),
//                    accumuloStore.getProperties().getTable(),
//                    Utils.serialiseConfiguration(getConfiguration(operation)));
//            final RDD<Element> elementRDD = rdd
//                    .mapPartitions(new EntryIteratorToElementIterator(accumuloStore), true, ELEMENT_CLASS_TAG);
//            return elementRDD.toJavaRDD();
//        } catch (final IOException e) {
//            throw new OperationException("IOException creating RFileReaderRDD", e);
//        }
//    }

//    public class EntryIteratorToElementIterator implements Function1<Iterator<Map.Entry<Key, Value>>, Iterator<Element>> {
//        private AccumuloStore accumuloStore;
//
//        public EntryIteratorToElementIterator(final AccumuloStore accumuloStore) {
//            this.accumuloStore = accumuloStore;
//        }
//
//        @Override
//        public Iterator<Element> apply(final Iterator<Map.Entry<Key, Value>> entryIterator) {
////            final AccumuloElementConverter converter = accumuloStore.getKeyPackage().getKeyConverter();
//            final EntryToElement entryToElement = new EntryToElement(accumuloStore);
//            return entryIterator.map(entryToElement);
//        }
//    }

//    public class EntryToElement implements Function1<Map.Entry<Key, Value>, Element> {
////        private AccumuloStore accumuloStore;
//        private AccumuloElementConverter converter;
//
//        public EntryToElement(final AccumuloStore accumuloStore) {
////            this.accumuloStore = accumuloStore;
//            this.converter = accumuloStore.getKeyPackage().getKeyConverter();
//        }
//
//        @Override
//        public Element apply(final Map.Entry<Key, Value> entry) {
//            return converter.getFullElement(entry.getKey(), entry.getValue());
//        }
//    }

    static class FirstElement implements Function<Tuple2<Element, NullWritable>, Element> {

        private static final long serialVersionUID = -4695668644733530293L;

        @Override
        public Element call(final Tuple2<Element, NullWritable> tuple) throws Exception {
            return tuple._1();
        }
    }
}
