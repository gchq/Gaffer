/*
 * Copyright 2017 Crown Copyright
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

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.rdd.OrderedRDDFunctions;
import org.apache.spark.rdd.PairRDDFunctions;
import scala.Tuple2;
import scala.math.Ordering;
import scala.math.Ordering$;
import scala.reflect.ClassTag;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation.ImportAccumuloKeyValueFiles;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.scalardd.ImportKeyValuePairRDDToAccumulo;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.utils.AccumuloKeyRangePartitioner;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Comparator;

public class ImportKeyValuePairRDDToAccumuloHandler implements OperationHandler<ImportKeyValuePairRDDToAccumulo, Void> {

    private static final ClassTag<Key> KEY_CLASS_TAG = scala.reflect.ClassTag$.MODULE$.apply(Key.class);
    private static final ClassTag<Value> VALUE_CLASS_TAG = scala.reflect.ClassTag$.MODULE$.apply(Value.class);
    private static final Ordering<Key> ORDERING_CLASS_TAG = Ordering$.MODULE$.comparatorToOrdering(Comparator.<Key>naturalOrder());

    @Override
    public Void doOperation(final ImportKeyValuePairRDDToAccumulo operation, final Context context, final Store store) throws OperationException {
        String outputPath = operation.getOutputPath();
        if (null == outputPath || outputPath.isEmpty()) {
            throw new OperationException("Option outputPath must be set for this option to be run against the accumulostore");
        }
        String failurePath = operation.getFailurePath();
        if (null == failurePath || failurePath.isEmpty()) {
            throw new OperationException("Option failurePath must be set for this option to be run against the accumulostore");
        }
        doOperation(operation, context, (AccumuloStore) store);
        return null;
    }

    private void doOperation(final ImportKeyValuePairRDDToAccumulo operation, final Context context, final AccumuloStore store) throws OperationException {
        AccumuloKeyRangePartitioner partitioner = new AccumuloKeyRangePartitioner(store);
        OrderedRDDFunctions orderedRDDFunctions = new OrderedRDDFunctions(operation.getInput(), ORDERING_CLASS_TAG, KEY_CLASS_TAG, VALUE_CLASS_TAG, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class));
        PairRDDFunctions pairRDDFunctions = new PairRDDFunctions(orderedRDDFunctions.repartitionAndSortWithinPartitions(partitioner), KEY_CLASS_TAG, VALUE_CLASS_TAG, ORDERING_CLASS_TAG);
        pairRDDFunctions.saveAsNewAPIHadoopFile(operation.getOutputPath(), Key.class, Value.class, AccumuloFileOutputFormat.class, getConfiguration(operation));
        ImportAccumuloKeyValueFiles importAccumuloKeyValueFiles = new ImportAccumuloKeyValueFiles.Builder().inputPath(operation.getOutputPath()).failurePath(operation.getFailurePath()).build();
        store.execute(importAccumuloKeyValueFiles, context.getUser());
    }

    protected Configuration getConfiguration(final Operation operation) throws OperationException {
        final Configuration conf = new Configuration();
        final String serialisedConf = operation.getOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY);
        if (serialisedConf != null) {
            try {
                final ByteArrayInputStream bais = new ByteArrayInputStream(serialisedConf.getBytes(CommonConstants.UTF_8));
                conf.readFields(new DataInputStream(bais));
            } catch (final IOException e) {
                throw new OperationException("Exception decoding Configuration from options", e);
            }
        }
        return conf;
    }
}
