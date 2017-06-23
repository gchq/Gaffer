/*
 * Copyright 2017. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.operation.getelements.handler;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.spark.SparkUser;
import uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.user.User;

public class GetDataframeOfElementsHandler implements OutputOperationHandler<GetDataFrameOfElements, Dataset<Row>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetDataframeOfElementsHandler.class);

    @Override
    public Dataset<Row> doOperation(final GetDataFrameOfElements operation,
                                    final Context context,
                                    final Store store) throws OperationException {
        final User user = context.getUser();
        final SparkSession spark;
        if (user instanceof SparkUser) {
            spark = ((SparkUser) user).getSparkSession();
        } else {
            throw new OperationException("This operation requires the user to be of type SparkUser.");
        }
        return doOperation(operation, (ParquetStore) store, spark);

    }

    private Dataset<Row> doOperation(final GetDataFrameOfElements operation,
                                     final ParquetStore store,
                                     final SparkSession spark) throws OperationException {
        if (operation.getView().equals(store.getSchemaUtils().getEmptyView())) {
            LOGGER.info("Retrieving elements as a dataframe");
            final ParquetStoreProperties props = store.getProperties();
            final Dataset<Row> dataset = spark
                    .read()
                    .option("mergeSchema", true)
                    .parquet(props.getDataDir() + "/" + store.getCurrentSnapshot() + "/graph");
            LOGGER.info("The merged schema that the data is being loaded using is: {}", dataset.schema().treeString());
            return dataset;
        } else {
            throw new OperationException("Views are not supported by this operation yet");
        }
    }
}
