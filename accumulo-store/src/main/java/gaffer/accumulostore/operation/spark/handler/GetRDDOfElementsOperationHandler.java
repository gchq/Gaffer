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
package gaffer.accumulostore.operation.spark.handler;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.inputformat.ElementInputFormat;
import gaffer.data.element.Element;
import gaffer.operation.OperationException;
import gaffer.operation.simple.spark.GetRDDOfElementsOperation;
import gaffer.store.Context;
import gaffer.store.Store;
import gaffer.store.operation.handler.OperationHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.Collections;

public class GetRDDOfElementsOperationHandler
        extends AbstractGetRDDOperationHandler
        implements OperationHandler<GetRDDOfElementsOperation, Iterable<RDD<Element>>> {

    @Override
    public Iterable<RDD<Element>> doOperation(final GetRDDOfElementsOperation operation,
                                              final Context context,
                                              final Store store)
            throws OperationException {
        return doOperation(operation, context, (AccumuloStore) store);
    }

    public Iterable<RDD<Element>> doOperation(final GetRDDOfElementsOperation operation,
                                              final Context context,
                                              final AccumuloStore accumuloStore)
            throws OperationException {
        final SparkContext sparkContext = operation.getSparkContext();
        final Configuration conf = getConfiguration(operation);
        addIterators(accumuloStore, conf, operation);
        addRanges(accumuloStore, conf, operation);
        final RDD<Tuple2<Element, NullWritable>> pairRDD = sparkContext.newAPIHadoopRDD(conf,
                ElementInputFormat.class,
                Element.class,
                NullWritable.class);
        final RDD<Element> rdd = pairRDD.map(new FirstElement(), ClassTagConstants.ELEMENT_CLASS_TAG);
        return Collections.singleton(rdd);
    }

}
