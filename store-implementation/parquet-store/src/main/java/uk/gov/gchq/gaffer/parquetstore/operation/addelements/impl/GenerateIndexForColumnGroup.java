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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.index.ColumnIndex;
import uk.gov.gchq.gaffer.parquetstore.index.MinMaxPath;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.koryphe.tuple.n.Tuple4;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Generates the index for a single group directory.
 */
public class GenerateIndexForColumnGroup implements Callable<Tuple4<String, String, ColumnIndex, OperationException>>, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(GenerateIndexForColumnGroup.class);
    private static final long serialVersionUID = 2287226248631201061L;
    private final String directoryPath;
    private final String[] paths;
    private final ColumnIndex columnIndex;
    private final String group;
    private final String column;

    public GenerateIndexForColumnGroup(final String directoryPath, final String[] paths, final String group, final String column) throws OperationException,
            SerialisationException, StoreException {
        this.directoryPath = directoryPath;
        this.paths = paths.clone();
        this.columnIndex = new ColumnIndex();
        this.group = group;
        this.column = column;
    }

    @Override
    public Tuple4<String, String, ColumnIndex, OperationException> call() {
        try {
            final FileSystem fs = FileSystem.get(new Configuration());
            if (fs.exists(new Path(directoryPath))) {
                    final FileStatus[] files = fs.listStatus(new Path(directoryPath),
                            path1 -> path1.getName().endsWith(".parquet"));
                    Pair<int[], String[]> colIndexWithTags = null;
                    for (final FileStatus file : files) {
                        final ParquetMetadata parquetMetadata = ParquetFileReader
                                .readFooter(fs.getConf(), file, ParquetMetadataConverter.NO_FILTER);
                        List<BlockMetaData> blocks = parquetMetadata.getBlocks();
                        if (blocks.size() > 0) {
                            if (colIndexWithTags == null) {
                                colIndexWithTags = getColumnIndexesWithTags(paths, parquetMetadata);
                            }
                            columnIndex.add(generateGafferObjectsIndex(colIndexWithTags, blocks, file.getPath().getName()));
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

    private Pair<int[], String[]> getColumnIndexesWithTags(final String[] paths, final ParquetMetadata parquetMetadata) {
        final List<ColumnChunkMetaData> columnChunks = parquetMetadata.getBlocks().get(0).getColumns();
        final int[] columnindexes = new int[paths.length];
        final String[] tags = new String[paths.length];
        for (int i = 0; i < paths.length; i++) {
            final String path = paths[i];
            LOGGER.debug("path: {}", path);
            for (int n = 0; n < columnChunks.size(); n++) {
                if (columnChunks.get(n).getPath().toDotString().equals(path)) {
                    columnindexes[i] = n;
                    final OriginalType tag = parquetMetadata.getFileMetaData().getSchema().getFields().get(n).getOriginalType();
                    if (tag != null) {
                        tags[i] = tag.name();
                    }
                    break;
                }
            }
        }
        return new Pair<>(columnindexes, tags);
    }

    private MinMaxPath generateGafferObjectsIndex(final Pair<int[], String[]> colIndexesWithTags,
                                            final List<BlockMetaData> blocks, final String path) throws StoreException {
        final int[] colIndexes = colIndexesWithTags.getFirst();
        final String[] tags = colIndexesWithTags.getSecond();
        final int numOfCols = colIndexes.length;
        final Object[] min = new Object[numOfCols];
        final Object[] max = new Object[numOfCols];
        for (int i = 0; i < numOfCols; i++) {
            final int colIndex = colIndexes[i];
            final ColumnChunkMetaData minColumn = blocks.get(0).getColumns().get(colIndex);
            final String parquetColumnType = minColumn.getType().javaType.getSimpleName();
            final String tag = tags[i];
            final Object minParquetObject = minColumn.getStatistics().genericGetMin();
            final Object maxParquetObject = blocks.get(blocks.size() - 1).getColumns().get(colIndex).getStatistics().genericGetMax();
            if ("Binary".equals(parquetColumnType)) {
                if ("UTF8".equals(tag)) {
                    min[i] = ((Binary) minParquetObject).toStringUsingUTF8();
                    max[i] = ((Binary) maxParquetObject).toStringUsingUTF8();
                } else {
                    min[i] = ((Binary) minParquetObject).getBytes();
                    max[i] = ((Binary) maxParquetObject).getBytes();
                }
            } else {
                min[i] = minParquetObject;
                max[i] = maxParquetObject;
            }
        }
        return new MinMaxPath(min, max, path);
    }
}
