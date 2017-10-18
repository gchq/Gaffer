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

package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.graphframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.graphframes.GraphFrame;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.operation.graphframe.GetGraphFrameOfElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.dataframe.AccumuloStoreRelation;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@code GetGraphFrameOfElementsHandler} handles {@link GetGraphFrameOfElements}
 * operations.
 *
 * The implementation found here is very similar to the {@link uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.dataframe.GetDataFrameOfElementsHandler}
 * implementation. The main difference is that the resulting {@link Dataset} of
 * elements are split into two {@link Dataset}s based on the groups provided in
 * the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.View}.
 *
 * @see uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.dataframe.GetDataFrameOfElementsHandler
 */
public class GetGraphFrameOfElementsHandler implements OutputOperationHandler<GetGraphFrameOfElements, GraphFrame> {

    @Override
    public GraphFrame doOperation(final GetGraphFrameOfElements operation, final Context context, final Store store) throws OperationException {
        return doOperation(operation, context, (AccumuloStore) store);
    }

    public GraphFrame doOperation(final GetGraphFrameOfElements operation, final Context context,
                                  final AccumuloStore store) throws OperationException {
        final Map<String, String> operationOptions;
        if (operation.getOptions() != null) {
            operationOptions = operation.getOptions();
        } else {
            operationOptions = new HashMap<>();
        }
        final AccumuloStoreRelation relation = new AccumuloStoreRelation(context,
                operation.getConverters(),
                operation.getView(),
                store,
                operationOptions);

        final Dataset<Row> elements = relation.sqlContext().baseRelationToDataFrame(relation);

        elements.createOrReplaceTempView("elements");

        final String edgeGroups = groupsToString(operation.getView().getEdgeGroups());
        final String entityGroups = groupsToString(operation.getView().getEntityGroups());

        final Dataset<Row> edges = relation.sqlContext().sql("select * from elements where group in " + edgeGroups)
                .withColumn("id", functions.row_number().over(Window.orderBy("group").partitionBy("group")));

        final Dataset<Row> entities = relation.sqlContext().sql("select * from elements where group in " + entityGroups);

        return GraphFrame.apply(entities.withColumnRenamed("vertex", "id"), edges);
    }

    private String groupsToString(final Set<String> groups) {
        return groups.stream()
                .collect(Collectors.joining("\',\'", "(\'", "\')"));
    }
}
