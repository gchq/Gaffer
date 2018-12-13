/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.hadoop;

import org.apache.accumulo.core.client.mapred.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.SparkContextUtil;
import uk.gov.gchq.gaffer.spark.operation.hadoop.GetHadoopConfiguration;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

public class GetHadoopConfigurationHandler extends AbstractGetRDDHandler<GetHadoopConfiguration, Configuration> {


    public Configuration doOperation(final GetHadoopConfiguration operation, final Context context, final Store store) throws OperationException {
        return doOperation(operation, context, (AccumuloStore) store);
    }

    private Configuration doOperation(final GetHadoopConfiguration operation,
                                      final Context context,
                                      final AccumuloStore accumuloStore)
            throws OperationException {
        SparkSession sparkSession = SparkContextUtil.getSparkSession(context, accumuloStore.getProperties());
        if (sparkSession == null) {
            throw new OperationException("This operation requires an active SparkSession.");
        }
        sparkSession.sparkContext().hadoopConfiguration().addResource(getConfiguration(operation));

        return doOperationUsingElementInputFormat(operation, context, accumuloStore);
    }

    private Configuration doOperationUsingElementInputFormat(final GetHadoopConfiguration operation,
                                                             final Context context,
                                                             final AccumuloStore accumuloStore)
            throws OperationException {

        final Configuration conf = getConfiguration(operation);
        addIterators(accumuloStore, conf, context.getUser(), operation);
        final String useBatchScannerRDD = operation.getOption(USE_BATCH_SCANNER_RDD);
        if (Boolean.parseBoolean(useBatchScannerRDD)) {
            InputConfigurator.setBatchScan(AccumuloInputFormat.class, conf, true);
        }
        if (conf.get("instance.zookeeper.host") == null) {
            conf.set("instance.zookeeper.host", accumuloStore.getProperties().get("accumulo.zookeepers").replace("\\n", ""));
        }

        return conf;
    }
}
