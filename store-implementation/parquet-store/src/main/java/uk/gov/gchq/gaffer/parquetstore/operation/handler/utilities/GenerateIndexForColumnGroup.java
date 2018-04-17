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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.mutable.Builder;
import scala.collection.mutable.Seq;
import scala.collection.mutable.Seq$;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.index.ColumnIndex;
import uk.gov.gchq.gaffer.parquetstore.index.MinValuesWithPath;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.koryphe.tuple.n.Tuple4;

import java.io.IOException;
import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;

/**
 * Generates the index for a single group directory.
 */
public class GenerateIndexForColumnGroup implements Callable<Tuple4<String, String, ColumnIndex, OperationException>>, Serializable {
    private static final long serialVersionUID = 2287226248631201061L;
    private final String directoryPath;
    private final String[] paths;
    private final ColumnIndex columnIndex;
    private final String group;
    private final String column;
    private final SparkSession spark;

    public GenerateIndexForColumnGroup(final String directoryPath, final String[] paths, final String group, final String column, final SparkSession spark) throws OperationException,
            SerialisationException, StoreException {
        this.directoryPath = directoryPath;
        this.paths = paths;
        this.columnIndex = new ColumnIndex();
        this.group = group;
        this.column = column;
        this.spark = spark;
    }

    @Override
    public Tuple4<String, String, ColumnIndex, OperationException> call() {
        try {
            final FileSystem fs = FileSystem.get(new Configuration());
            if (fs.exists(new Path(directoryPath))) {
                final FileStatus[] files = fs.listStatus(new Path(directoryPath),
                        path1 -> path1.getName().endsWith(".parquet"));
                for (final FileStatus file : files) {
                    final String firstColumn = paths[0];
                    final Builder<String, Seq<String>> seqBuilder = Seq$.MODULE$.newBuilder();
                    final int numberOfColumns = paths.length;
                    for (int i = 1; i < numberOfColumns; i++) {
                        seqBuilder.$plus$eq(paths[i]);
                    }
                    final Dataset<Row> fileData = spark.read().parquet(file.getPath().toString()).select(firstColumn, seqBuilder.result());
                    try {
                        final Row minRow = fileData.head();
                        columnIndex.add(generateGafferObjectsIndex(minRow, file.getPath().getName()));
                    } catch (final NoSuchElementException ignored) {
                        // ignore as dataframe was empty
                    }
                }
            }
        } catch (final IOException e) {
            return new Tuple4<>(group, column, null, new OperationException("IOException generating the index files", e));
        } catch (final StoreException e) {
            return new Tuple4<>(group, column, null, new OperationException(e.getMessage()));
        }
        return new Tuple4<>(group, column, columnIndex, null);
    }

    private MinValuesWithPath generateGafferObjectsIndex(final Row minRow, final String path) throws StoreException {
        final int numOfCols = minRow.length();
        final Object[] min = new Object[numOfCols];
        for (int i = 0; i < numOfCols; i++) {
            min[i] = minRow.get(i);
        }
        return new MinValuesWithPath(min, path);
    }
}
