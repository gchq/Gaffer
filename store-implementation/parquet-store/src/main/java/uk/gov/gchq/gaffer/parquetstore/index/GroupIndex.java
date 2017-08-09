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
package uk.gov.gchq.gaffer.parquetstore.index;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.store.StoreException;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to store a file-based index for a single group, i.e. for each group it stores a set of
 * {@link ColumnIndex}s. Each {@link ColumnIndex} contains a set of {@link MinValuesWithPath} which contains the path to a file
 * that contains data for that group along with minimum and maximum values of the indexed columns within that file.
 * This allows queries for particular values of the indexed columns to skip files that do not contain relevant data.
 */
public class GroupIndex {
    private final HashMap<String, ColumnIndex> columnToIndex;

    public GroupIndex() {
        columnToIndex = new HashMap<>();
    }

    public ColumnIndex getColumn(final String column) {
        return columnToIndex.get(column);
    }

    public Set<String> columnsIndexed() {
        return Collections.unmodifiableSet(columnToIndex.keySet());
    }

    public void add(final String column, final ColumnIndex columnIndex) {
        if (columnToIndex.containsKey(column)) {
            throw new IllegalArgumentException("Cannot overwrite an entry in an index (column was " + column + ")");
        }
        columnToIndex.put(column, columnIndex);
    }

    public void writeColumns(final String group, final String rootDir, final FileSystem fs) throws StoreException {
        try {
            for (final Map.Entry<String, ColumnIndex> columnIndexEntry : columnToIndex.entrySet()) {
                final String column = columnIndexEntry.getKey();
                final ColumnIndex colIndex = columnIndexEntry.getValue();
                final String indexDir = ParquetStore.getGroupDirectory(group, column, rootDir) + "/";
                final Path path = new Path(indexDir + ParquetStoreConstants.INDEX);
                colIndex.write(fs.create(path));
            }
        } catch (final IOException e) {
            throw new StoreException(e.getMessage());
        }
    }

    public void readColumns(final String group, final String rootDir, final FileSystem fs) throws StoreException {
        try {
            final String[] columns = new String[]{ParquetStoreConstants.VERTEX, ParquetStoreConstants.SOURCE, ParquetStoreConstants.DESTINATION};
            for (final String column : columns) {
                final String indexDir = ParquetStore.getGroupDirectory(group, column, rootDir) + "/";
                final Path path = new Path(indexDir + ParquetStoreConstants.INDEX);
                if (fs.exists(path)) {
                    final ColumnIndex colIndex = new ColumnIndex();
                    colIndex.read(fs.open(path));
                    add(column, colIndex);
                }
            }
        } catch (final IOException e) {
            throw new StoreException(e.getMessage());
        }
    }
}
