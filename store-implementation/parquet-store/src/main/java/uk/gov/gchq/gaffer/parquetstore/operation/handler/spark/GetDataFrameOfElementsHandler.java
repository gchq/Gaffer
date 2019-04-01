/*
 * Copyright 2017-2019. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.operation.handler.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.spark.SparkContextUtil;
import uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.io.Serializable;

/**
 * An {@link uk.gov.gchq.gaffer.store.operation.handler.OperationHandler} for the {@link GetDataFrameOfElements}
 * operation on the {@link ParquetStore}.
 */
public class GetDataFrameOfElementsHandler implements OutputOperationHandler<GetDataFrameOfElements, Dataset<Row>>, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetDataFrameOfElementsHandler.class);
    private static final long serialVersionUID = 6355341028414862100L;

    @Override
    public Dataset<Row> doOperation(final GetDataFrameOfElements operation,
                                    final Context context,
                                    final Store store) throws OperationException {
        final SparkSession spark = SparkContextUtil.getSparkSession(context, store.getProperties());
        return doOperation(operation, (ParquetStore) store, spark);
    }

    private Dataset<Row> doOperation(final GetDataFrameOfElements operation,
                                     final ParquetStore store,
                                     final SparkSession spark) throws OperationException {
        if (!operation.getView()
                .equals(new View.Builder()
                        .entities(store.getSchema().getEntityGroups())
                        .edges(store.getSchema().getEdgeGroups()).build())) {
            throw new OperationException("This operation does not currently support views");
        }
        LOGGER.debug("Creating a Dataset<Row> from path {} with option mergeSchema=true", store.getGraphPath());

        final StructType schema = new SchemaUtils(store.getSchema()).getMergedSparkSchema(store.getSchema().getGroups());
        final Dataset<Row> dataframe = spark
                .read()
                .schema(schema)
                .parquet(store.getGraphPath());
        return dataframe;
    }
}
