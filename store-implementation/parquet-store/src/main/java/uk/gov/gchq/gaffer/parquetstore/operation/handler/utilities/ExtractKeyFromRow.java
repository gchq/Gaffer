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

package uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import scala.collection.Seq;
import scala.collection.Seq$;
import scala.collection.mutable.Builder;

import uk.gov.gchq.gaffer.parquetstore.ParquetStore;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to extract the key from each {@link Row} using the set of groupBy column names.
 */
public class ExtractKeyFromRow implements Function<Row, Seq<Object>> {

    private static final long serialVersionUID = -5811180619204002981L;
    private final Set<String> groupByColumns;

    public ExtractKeyFromRow(final Set<String> groupByColumns,
                             final Map<String, String[]> columnToPaths,
                             final boolean isEntity,
                             final boolean isReversed) {
        this.groupByColumns = new LinkedHashSet<>();
        if (isEntity) {
            addGroupByColumns(columnToPaths, ParquetStore.VERTEX);
        } else if (!isReversed) {
            addGroupByColumns(columnToPaths, ParquetStore.SOURCE);
            addGroupByColumns(columnToPaths, ParquetStore.DESTINATION);
            this.groupByColumns.add(ParquetStore.DIRECTED);
        } else {
            addGroupByColumns(columnToPaths, ParquetStore.DESTINATION);
            addGroupByColumns(columnToPaths, ParquetStore.SOURCE);
            this.groupByColumns.add(ParquetStore.DIRECTED);
        }
        for (final String col : columnToPaths.keySet()) {
            if (groupByColumns.contains(col)) {
                addGroupByColumns(columnToPaths, col);
            }
        }
    }

    private void addGroupByColumns(final Map<String, String[]> columnToPaths, final String col) {
        final String[] paths = columnToPaths.get(col);
        if (null != paths) {
            if (paths[0].contains(".")) {
                groupByColumns.add(col);
            } else {
                Collections.addAll(groupByColumns, paths);
            }
        }
    }

    @Override
    public Seq<Object> call(final Row row) {
        final Builder<Object, Seq<Object>> key = Seq$.MODULE$.newBuilder();
        for (final String column : groupByColumns) {
            final Object columnValue = row.getAs(column);
            if (columnValue instanceof byte[]) {
                key.$plus$eq(Arrays.toString((byte[]) columnValue));
            } else {
                key.$plus$eq(columnValue);
            }
        }
        return key.result();
    }
}
