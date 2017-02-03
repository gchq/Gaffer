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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.javardd;

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.scalardd.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.javardd.ImportKeyValueJavaPairRDDToAccumulo;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.utils.AccumuloKeyRangePartitioner;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class ImportKeyValueJavaPairRDDToAccumuloHandler implements OperationHandler<ImportKeyValueJavaPairRDDToAccumulo, Void> {

    @Override
    public Void doOperation(final ImportKeyValueJavaPairRDDToAccumulo operation, final Context context, final Store store) throws OperationException {
        doOperation(operation, (AccumuloStore) store);
        return null;
    }

    public void doOperation(final ImportKeyValueJavaPairRDDToAccumulo operation, final AccumuloStore store) throws OperationException {
        importRdd(operation, store);
    }

    private void importRdd(final ImportKeyValueJavaPairRDDToAccumulo operation, final AccumuloStore store) throws OperationException {
        AccumuloKeyRangePartitioner partitioner = new AccumuloKeyRangePartitioner(store.getProperties());
        JavaPairRDD<Key, Value> rdd = operation.getInput();
        rdd = rdd.repartitionAndSortWithinPartitions(partitioner);
        rdd.saveAsNewAPIHadoopFile(operation.getOutputPath(), Key.class, Value.class, AccumuloFileOutputFormat.class, getConfiguration(operation));
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
