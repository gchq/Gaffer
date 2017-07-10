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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.OriginalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.store.StoreException;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;

public class GenerateIndexForGroup implements Callable<OperationException>, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(GenerateIndexForGroup.class);
    private static final long serialVersionUID = 2287226248631201061L;
    private final String directoryPath;
    private final String[] paths;

    public GenerateIndexForGroup(final String directoryPath, final String[] paths) throws OperationException,
            SerialisationException, StoreException {
        this.directoryPath = directoryPath;
        this.paths = paths.clone();
    }

    @Override
    public OperationException call() {
        try {
            final FileSystem fs = FileSystem.get(new Configuration());
            if (fs.exists(new Path(directoryPath))) {
                try (final FSDataOutputStream outputFile = fs.create(new Path(directoryPath + "/" + ParquetStoreConstants.INDEX))) {
                    final FileStatus[] files = fs.listStatus(new Path(directoryPath),
                            path1 -> path1.getName().endsWith(".parquet"));
                    int[] colIndex = null;
                    for (final FileStatus file : files) {
                        final ParquetMetadata parquetMetadata = ParquetFileReader
                                .readFooter(fs.getConf(), file, ParquetMetadataConverter.NO_FILTER);
                        List<BlockMetaData> blocks = parquetMetadata.getBlocks();
                        if (blocks.size() > 0) {
                            if (colIndex == null) {
                                colIndex = getColumnIndexes(paths, parquetMetadata);
                            }
                            generateGafferObjectsIndex(outputFile, colIndex, blocks);
                            byte[] filePath = StringUtil.toBytes(file.getPath().getName());
                            outputFile.writeInt(filePath.length);
                            outputFile.write(filePath);
                        }
                    }
                    outputFile.hsync();
                }
            }
        } catch (final IOException e) {
            return new OperationException("IOException generating the index files", e);
        }
        return null;
    }

    private int[] getColumnIndexes(final String[] paths, final ParquetMetadata parquetMetadata) {
        final List<ColumnChunkMetaData> columnChunks = parquetMetadata.getBlocks().get(0).getColumns();
        final int[] columnindexes = new int[paths.length];
        for (int i = 0; i < paths.length; i++) {
            final String path = paths[i];
            LOGGER.debug("path: {}", path);
            for (int n = 0; n < columnChunks.size(); n++) {
                if (columnChunks.get(n).getPath().toDotString().equals(path)) {
                    final OriginalType type = parquetMetadata.getFileMetaData().getSchema().getFields().get(n).getOriginalType();
                    if (type != null && "UTF8".equals(type.name())) {
                        columnindexes[i] = -n;
                    } else {
                        columnindexes[i] = n;
                    }

                    break;
                }
            }
        }
        return columnindexes;
    }

    private void generateGafferObjectsIndex(final FSDataOutputStream outputFile, final int[] colIndexes,
                                            final List<BlockMetaData> blocks) throws IOException {
        outputFile.writeInt(colIndexes.length);
        for (final int colIndex : colIndexes) {
            LOGGER.debug("colIndex: {}", colIndex);
            final boolean isString;
            final int updatedColIndex;
            if (colIndex < 0) {
                isString = true;
                updatedColIndex = -colIndex;
            } else {
                updatedColIndex = colIndex;
                isString = false;
            }
            final ColumnChunkMetaData minColumn = blocks.get(0).getColumns().get(updatedColIndex);
            String columnTypeString = minColumn.getType().javaType.getCanonicalName();
            if (isString) {
                columnTypeString = columnTypeString + " (UTF8)";
            }
            final byte[] columnType = StringUtil.toBytes(columnTypeString);
            outputFile.writeInt(columnType.length);
            outputFile.write(columnType);
            final byte[] minValue = minColumn.getStatistics().getMinBytes();
            outputFile.writeInt(minValue.length);
            outputFile.write(minValue);
            final byte[] maxValue = blocks.get(blocks.size() - 1).getColumns().get(updatedColIndex).getStatistics().getMaxBytes();
            outputFile.writeInt(maxValue.length);
            outputFile.write(maxValue);
        }
    }
}
