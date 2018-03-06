/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.spark.utils.scala;

import com.google.common.collect.Sets;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Set;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;


/**
 * Utility class for manipulating DataFrames.
 */
public final class DataFrameUtil {
    private DataFrameUtil() {
    }

    /**
     * Use pattern matching to fill a column with the corresponding value (if it
     * exists), otherwise null.
     *
     * @param cols    the columns to fill in
     * @param allCols a set containing all columns of interest
     * @return a list containing the filled columns
     */
    private static Column[] expr(final Set<String> cols, final Set<String> allCols) {
        return allCols.stream()
                .map(x -> {
                    if (cols.contains(x)) {
                        return col(x);
                    } else {
                        return lit(null).as(x);
                    }
                }).toArray(Column[]::new);
    }

    /**
     * Carry out a union of two {@link Dataset}s where the input
     * Datasets may contain a different number of columns.
     * The resulting Dataset will contain entries for all of the columns found in
     * the input Dataset, with null entries used as placeholders.
     *
     * @param ds1 the first Dataset
     * @param ds2 the second Dataset
     * @return the combined Dataset
     */
    public static Dataset<Row> union(final Dataset<Row> ds1, final Dataset<Row> ds2) {
        Set<String> ds1Cols = Sets.newHashSet(ds1.columns());
        Set<String> ds2Cols = Sets.newHashSet(ds2.columns());

        final Set<String> total = Sets.newHashSet(ds1Cols);
        total.addAll(ds2Cols);

        return ds1.select(expr(ds1Cols, total)).union(ds2.select(expr(ds2Cols, total)));
    }

    /**
     * Create an empty {@link Dataset} of {@link Row}s for use as edges in a {@link org.graphframes.GraphFrame}.
     *
     * @param sparkSession the spark session
     * @return an empty {@link Dataset} of {@link Row}s with a src and dst column.
     */
    public static Dataset<Row> emptyEdges(final SparkSession sparkSession) {
        return sparkSession.emptyDataFrame().select(lit(null).as("src"), lit(null).as("dst"));
    }
}
