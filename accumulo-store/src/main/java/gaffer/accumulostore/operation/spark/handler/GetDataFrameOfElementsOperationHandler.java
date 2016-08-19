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
import gaffer.operation.OperationException;
import gaffer.operation.simple.spark.GetDataFrameOfElements;
import gaffer.store.Context;
import gaffer.store.Store;

import gaffer.store.operation.handler.OperationHandler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Collections;

public class GetDataFrameOfElementsOperationHandler implements OperationHandler<GetDataFrameOfElements, Iterable<Dataset<Row>>> {

    @Override
    public Iterable<Dataset<Row>> doOperation(final GetDataFrameOfElements operation, final Context context,
                                                 final Store store) throws OperationException {
        return doOperation(operation, context, (AccumuloStore) store);
    }

    public Iterable<Dataset<Row>> doOperation(final GetDataFrameOfElements operation, final Context context,
                                           final AccumuloStore store) throws OperationException {
        final SQLContext sqlContext = operation.getSqlContext();
        final AccumuloStoreRelation relation = new AccumuloStoreRelation(sqlContext, operation.getGroup(), store,
                context.getUser());
        return Collections.singleton(sqlContext.baseRelationToDataFrame(relation));
    }

}
