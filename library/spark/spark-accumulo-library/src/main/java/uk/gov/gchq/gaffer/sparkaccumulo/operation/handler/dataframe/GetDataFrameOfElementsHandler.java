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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

public class GetDataFrameOfElementsHandler implements
        OperationHandler<GetDataFrameOfElements, Dataset<Row>> {

    @Override
    public Dataset<Row> doOperation(final GetDataFrameOfElements operation, final Context context,
                                    final Store store) throws OperationException {
        return doOperation(operation, context, (AccumuloStore) store);
    }

    public Dataset<Row> doOperation(final GetDataFrameOfElements operation, final Context context,
                                    final AccumuloStore store) throws OperationException {
        final SQLContext sqlContext = operation.getSqlContext();
        final AccumuloStoreRelation relation = new AccumuloStoreRelation(sqlContext,
                operation.getConverters(),
                operation.getView(),
                store,
                context.getUser());
        return sqlContext.baseRelationToDataFrame(relation);
    }

}
