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

package uk.gov.gchq.gaffer.spark.operation.handler.graphframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.graphframes.GraphFrame;

import uk.gov.gchq.gaffer.data.element.ReservedPropertyNames;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.SparkContextUtil;
import uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements;
import uk.gov.gchq.gaffer.spark.operation.dataframe.converter.schema.SchemaToStructTypeConverter;
import uk.gov.gchq.gaffer.spark.operation.graphframe.GetGraphFrameOfElements;
import uk.gov.gchq.gaffer.spark.utils.scala.DataFrameUtil;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 * A {@code GetGraphFrameOfElementsHandler} handles {@link GetGraphFrameOfElements}
 * operations.
 * </p>
 * <p>
 * The implementation delegates to {@link GetDataFrameOfElements} operation.
 * Then the resulting {@link Dataset} of elements are split into two {@link Dataset}s
 * based on the groups provided in the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.View}.
 * </p>
 */
public class GetGraphFrameOfElementsHandler implements OutputOperationHandler<GetGraphFrameOfElements, GraphFrame> {
    @Override
    public GraphFrame doOperation(final GetGraphFrameOfElements operation, final Context context, final Store store) throws OperationException {
        final GetDataFrameOfElements getDataFrame = new GetDataFrameOfElements.Builder()
                .converters(operation.getConverters())
                .view(operation.getView())
                .options(operation.getOptions())
                .build();

        Dataset<Row> elements = store.execute(getDataFrame, context);
        elements = renameColumns(elements);
        elements.createOrReplaceTempView("elements");

        final String edgeGroups = groupsToString(operation.getView().getEdgeGroups());
        final String entityGroups = groupsToString(operation.getView().getEntityGroups());

        final SparkSession sparkSession = SparkContextUtil.getSparkSession(context, store.getProperties());

        // Create a DataFrame of Edges - must add an "id" column which we fill with
        // the row number. We add a partitionBy on group to avoid creating a single
        // partition for all data.
        Dataset<Row> edges = sparkSession.sql("select * from elements where group in " + edgeGroups)
                .withColumn(SchemaToStructTypeConverter.ID, functions.row_number().over(Window.orderBy(SchemaToStructTypeConverter.GROUP).partitionBy(SchemaToStructTypeConverter.GROUP)));

        // Create a DataFrame of Entities
        Dataset<Row> entities = sparkSession.sql("select * from elements where " + SchemaToStructTypeConverter.GROUP + " in " + entityGroups);

        if (!edges.rdd().isEmpty()) {
            // We also add dummy entities for all vertices present in the edge dataset,
            // in case there are no corresponding Entities
            final Dataset<Row> sources = sparkSession.sql("select " + SchemaToStructTypeConverter.SRC_COL_NAME + " as " + SchemaToStructTypeConverter.VERTEX_COL_NAME + " from elements where " + SchemaToStructTypeConverter.GROUP + " in " + edgeGroups);
            final Dataset<Row> destinations = sparkSession.sql("select " + SchemaToStructTypeConverter.DST_COL_NAME + " as " + SchemaToStructTypeConverter.VERTEX_COL_NAME + " from elements where " + SchemaToStructTypeConverter.GROUP + " in " + edgeGroups);

            final Dataset<Row> vertices = sources.union(destinations).distinct();

            entities = DataFrameUtil.union(vertices, entities);
        } else {
            // If there are no edges, add an empty DataFrame
            edges = DataFrameUtil.emptyEdges(sparkSession);
        }

        return GraphFrame.apply(entities.withColumnRenamed(SchemaToStructTypeConverter.VERTEX_COL_NAME, SchemaToStructTypeConverter.ID), edges);
    }

    private Dataset<Row> renameColumns(final Dataset<Row> elements) {
        // Try to rename columns in case the Gaffer store uses different names.
        Dataset<Row> renamedElements = elements.withColumnRenamed(ReservedPropertyNames.GROUP.name(), SchemaToStructTypeConverter.GROUP);
        renamedElements = renamedElements.withColumnRenamed(ReservedPropertyNames.ID.name(), SchemaToStructTypeConverter.ID);
        renamedElements = renamedElements.withColumnRenamed(ReservedPropertyNames.SOURCE.name(), SchemaToStructTypeConverter.SRC_COL_NAME);
        renamedElements = renamedElements.withColumnRenamed(ReservedPropertyNames.DESTINATION.name(), SchemaToStructTypeConverter.DST_COL_NAME);
        renamedElements = renamedElements.withColumnRenamed(ReservedPropertyNames.DIRECTED.name(), SchemaToStructTypeConverter.DIRECTED_COL_NAME);
        renamedElements = renamedElements.withColumnRenamed(ReservedPropertyNames.VERTEX.name(), SchemaToStructTypeConverter.VERTEX_COL_NAME);
        renamedElements = renamedElements.withColumnRenamed(ReservedPropertyNames.MATCHED_VERTEX.name(), SchemaToStructTypeConverter.MATCHED_VERTEX_COL_NAME);
        return renamedElements;
    }

    private String groupsToString(final Set<String> groups) {
        return groups.stream()
                .collect(Collectors.joining("\',\'", "(\'", "\')"));
    }
}
