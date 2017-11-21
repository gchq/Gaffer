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

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.Authorisations;
import uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.ElementVisibility;
import uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.VisibilityEvaluator;
import uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.exception.VisibilityParseException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.spark.SparkContextUtil;
import uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.user.User;

import java.io.Serializable;

/**
 * An {@link uk.gov.gchq.gaffer.store.operation.handler.OperationHandler} for the {@link GetDataFrameOfElements}
 * operation on the {@link ParquetStore}.
 */
public class GetDataframeOfElementsHandler implements OutputOperationHandler<GetDataFrameOfElements, Dataset<Row>>, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetDataframeOfElementsHandler.class);
    private static final long serialVersionUID = 6355341028414862100L;

    @Override
    public Dataset<Row> doOperation(final GetDataFrameOfElements operation,
                                    final Context context,
                                    final Store store) throws OperationException {
        final SparkSession spark = SparkContextUtil.getSparkSession(context, store.getProperties());
        final User user = context.getUser();
        final Authorisations auths;
        final String visibility;
        if (user != null && user.getDataAuths() != null) {
            auths = new Authorisations(user.getDataAuths().toArray(new String[user.getDataAuths().size()]));
        } else {
            auths = new Authorisations();
        }

        if (store.getSchema().getVisibilityProperty() != null) {
            visibility = store.getSchema().getVisibilityProperty();
        } else {
            visibility = new String();
        }

        return doOperation(operation, (ParquetStore) store, spark, auths, visibility);
    }

    private Dataset<Row> doOperation(final GetDataFrameOfElements operation,
                                     final ParquetStore store,
                                     final SparkSession spark,
                                     final Authorisations auths,
                                     final String visibility) throws OperationException {
        if (operation.getView().equals(store.getSchemaUtils().getEmptyView())) {
            LOGGER.debug("Retrieving elements as a dataframe");
            final String rootDir;
            if (null != store.getGraphIndex()) {
                rootDir = store.getDataDir() + "/" + store.getGraphIndex().getSnapshotTimestamp() + "/";
            } else {
                return spark.emptyDataFrame();
            }

            final Dataset<Row> dataset;
            if (!visibility.isEmpty()) {
                final FilterFunction<Row> filter = e -> isVisible(e, visibility, auths);
                dataset = spark
                        .read()
                        .option("mergeSchema", true)
                        .parquet(rootDir + ParquetStoreConstants.GRAPH)
                        .filter(filter);
            } else {
                dataset = spark
                        .read()
                        .option("mergeSchema", true)
                        .parquet(rootDir + ParquetStoreConstants.GRAPH);
            }
            LOGGER.debug("The merged schema that the data is being loaded using is: {}", dataset.schema().treeString());
            return dataset;
        } else {
            throw new OperationException("Views are not supported by this operation yet");
        }
    }

    private boolean isVisible(final Row e, final String visibility, final Authorisations auths) throws VisibilityParseException {
        if (e.getAs(visibility) != null) {
            final VisibilityEvaluator visibilityEvaluator = new VisibilityEvaluator(auths);
            final ElementVisibility elementVisibility = new ElementVisibility((String) e.getAs(visibility));
            return visibilityEvaluator.evaluate(elementVisibility);
        } else {
            return true;
        }
    }
}
