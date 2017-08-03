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
package uk.gov.gchq.gaffer.parquetstore.operation.addelements.impl;

import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Row;

/**
 * Used when generating the indices to reduce the dataset to find the last row in a sorted file
 */
public class ReduceRowsToGetLastRow implements ReduceFunction<Row> {
    private final String[] columns;

    public ReduceRowsToGetLastRow(final String[] columns) {
        this.columns = columns;
    }

    @Override
    public Row call(final Row v1, final Row v2) throws Exception {
        for (final String column : columns) {
            final Comparable v1Value = v1.getAs(column);
            final Comparable v2Value = v2.getAs(column);
            final int comparison = v1Value.compareTo(v2Value);
            if (comparison > 0) {
                return v1;
            } else if (comparison < 0) {
                return v2;
            }
        }
        return v1;
    }
}
