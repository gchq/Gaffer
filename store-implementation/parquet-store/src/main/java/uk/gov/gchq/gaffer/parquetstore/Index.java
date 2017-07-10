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
package uk.gov.gchq.gaffer.parquetstore;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.store.StoreException;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * This class is used to store a file-based index for each group, i.e. for each group it stores a set of
 * {@link SubIndex}s. Each {@link SubIndex} contains a path to a file that contains data for that group along with
 * minimum and maximum values of the indexed columns within that file. This allows queries for particular values
 * of the indexed columns to skip files that do not contain relevant data.
 */
public class Index {
    private static final Logger LOGGER = LoggerFactory.getLogger(Index.class);
    private final Map<String, SubIndex> index;
    private long snapshot;

    public Index() {
        this.index = new HashMap<>();
    }

    public void add(final String group, final SubIndex subIndex) {
        if (index.containsKey(group)) {
            throw new IllegalArgumentException("Cannot overwrite an entry in an index (group was " + group + ")");
        }
        index.put(group, subIndex);
    }

    public Set<String> groupsIndexed() {
        return Collections.unmodifiableSet(index.keySet());
    }

    public SubIndex get(final String group) {
        return index.get(group);
    }

    public long getSnapshotTimestamp() {
        return snapshot;
    }

    public void loadIndices(final FileSystem fs,
                            final ParquetStore store) throws StoreException {
        LOGGER.info("Loading indices");
        try {
            final String rootDirString = store.getProperties().getDataDir();
            final Path rootDir = new Path(rootDirString);
            if (fs.exists(rootDir)) {
                final FileStatus[] snapshots = fs.listStatus(rootDir);
                long latestSnapshot = 0L;
                for (final FileStatus fileStatus : snapshots) {
                    final String snapshot = fileStatus.getPath().getName();
                    long currentSnapshot = Long.parseLong(snapshot);
                    if (latestSnapshot < currentSnapshot) {
                        latestSnapshot = currentSnapshot;
                    }
                }
                snapshot = latestSnapshot;
                LOGGER.info("Latest snapshot is {}", latestSnapshot);
                if (latestSnapshot != 0L) {
                    for (final String group : store.getSchemaUtils().getEntityGroups()) {
                        final Index.SubIndex subIndex = loadIndex(group, ParquetStoreConstants.VERTEX, latestSnapshot, rootDirString, fs);
                        if (!subIndex.isEmpty()) {
                            add(group, subIndex);
                        }
                    }
                    for (final String group : store.getSchemaUtils().getEdgeGroups()) {
                        final Index.SubIndex subIndex = loadIndex(group, ParquetStoreConstants.SOURCE, latestSnapshot, rootDirString, fs);
                        if (!subIndex.isEmpty()) {
                            add(group, subIndex);
                        }
                        final Index.SubIndex reverseSubIndex = loadIndex(group, ParquetStoreConstants.DESTINATION, latestSnapshot, rootDirString, fs);
                        if (!reverseSubIndex.isEmpty()) {
                            add(group + "_reversed", reverseSubIndex);
                        }
                    }
                }
            }
        } catch (final IOException e) {
            throw new StoreException("Failed to connect to the file system", e);
        }
    }

    private Index.SubIndex loadIndex(final String group,
                                     final String identifier,
                                     final long currentSnapshot,
                                     final String rootDataDir,
                                     final FileSystem fs) throws StoreException {
        try {
            final String indexDir;
            final Path path;
            if (ParquetStoreConstants.VERTEX.equals(identifier)) {
                indexDir = rootDataDir
                        + "/" + currentSnapshot
                        + "/" + ParquetStoreConstants.GRAPH
                        + "/" + ParquetStoreConstants.GROUP + "=" + group + "/";
                path = new Path(indexDir + ParquetStoreConstants.INDEX);
            } else if (ParquetStoreConstants.SOURCE.equals(identifier)) {
                indexDir = rootDataDir
                        + "/" + currentSnapshot
                        + "/" + ParquetStoreConstants.GRAPH
                        + "/" + ParquetStoreConstants.GROUP + "=" + group + "/";
                path = new Path(indexDir + ParquetStoreConstants.INDEX);
            } else {
                indexDir = rootDataDir
                        + "/" + currentSnapshot
                        + "/" + ParquetStoreConstants.REVERSE_EDGES
                        + "/" + ParquetStoreConstants.GROUP + "=" + group + "/";
                path = new Path(indexDir + ParquetStoreConstants.INDEX);
            }
            LOGGER.info("Loading the index from path {}", path);
            final Index.SubIndex subIndex = new Index.SubIndex();
            if (fs.exists(path)) {
                final FSDataInputStream reader = fs.open(path);
                while (reader.available() > 0) {
                    final int numOfCols = reader.readInt();
                    final Object[] minObjects = new Object[numOfCols];
                    final Object[] maxObjects = new Object[numOfCols];
                    for (int i = 0; i < numOfCols; i++) {
                        final int colTypeLength = reader.readInt();
                        final byte[] colType = readBytes(colTypeLength, reader);
                        final int minLength = reader.readInt();
                        final byte[] min = readBytes(minLength, reader);
                        minObjects[i] = deserialiseColumn(colType, min);
                        final int maxLength = reader.readInt();
                        final byte[] max = readBytes(maxLength, reader);
                        maxObjects[i] = deserialiseColumn(colType, max);
                    }
                    final int filePathLength = reader.readInt();
                    final byte[] filePath = readBytes(filePathLength, reader);
                    final String fileString = StringUtil.toString(filePath);
                    subIndex.add(new Index.MinMaxPath(minObjects, maxObjects, indexDir + fileString));
                }
            }
            return subIndex;
        } catch (final IOException e) {
            throw new StoreException("IOException while loading the index from " + rootDataDir
                    + "/" + currentSnapshot
                    + "/" + (ParquetStoreConstants.DESTINATION.equals(identifier) ?
                    ParquetStoreConstants.REVERSE_EDGES : ParquetStoreConstants.GRAPH)
                    + "/" + ParquetStoreConstants.GROUP + "=" + group
                    + "/" + ParquetStoreConstants.INDEX, e);
        }
    }

    private byte[] readBytes(final int length, final FSDataInputStream reader) throws IOException {
        final byte[] bytes = new byte[length];
        int bytesRead = 0;
        while (bytesRead < length && bytesRead > -1) {
            bytesRead += reader.read(bytes, bytesRead, length - bytesRead);
        }
        return bytes;
    }

    private Object deserialiseColumn(final byte[] colType, final byte[] value) {
        final String colTypeName = StringUtil.toString(colType);
        if ("long".equals(colTypeName)) {
            return BytesUtils.bytesToLong(value);
        } else if ("int".equals(colTypeName)) {
            return BytesUtils.bytesToInt(value);
        } else if ("boolean".equals(colTypeName)) {
            return BytesUtils.bytesToBool(value);
        } else if ("float".equals(colTypeName)) {
            return Float.intBitsToFloat(BytesUtils.bytesToInt(value));
        } else if (colTypeName.endsWith(" (UTF8)")) {
            return Binary.fromReusedByteArray(value).toStringUsingUTF8();
        } else {
            return value;
        }
    }

    public static class SubIndex {
        private static final Comparator<MinMaxPath> BY_PATH =
                (MinMaxPath mmp1, MinMaxPath mmp2) -> mmp1.getPath().compareTo(mmp2.getPath());
        private final SortedSet<MinMaxPath> minMaxPaths;

        public SubIndex() {
            this.minMaxPaths = new TreeSet<>(BY_PATH);
        }

        public boolean isEmpty() {
            return minMaxPaths.isEmpty();
        }

        public void add(final MinMaxPath minMaxPath) {
            minMaxPaths.add(minMaxPath);
        }

        public Iterator<MinMaxPath> getIterator() {
            return minMaxPaths.iterator();
        }
    }

    public static class MinMaxPath {
        private final Object[] min;
        private final Object[] max;
        private final String path;

        @SuppressFBWarnings(value = "EI_EXPOSE_REP2",
                justification = "This method is only used in this package and users will not mutate the values returned.")
        public MinMaxPath(final Object[] min, final Object[] max, final String path) {
            this.min = min;
            this.max = max;
            this.path = path;
        }

        @SuppressFBWarnings(value = "EI_EXPOSE_REP",
                justification = "This method is only used in this package and users will not mutate the values returned.")
        public Object[] getMin() {
            return min;
        }

        @SuppressFBWarnings(value = "EI_EXPOSE_REP",
                justification = "This method is only used in this package and users will not mutate the values returned.")
        public Object[] getMax() {
            return max;
        }

        public String getPath() {
            return path;
        }
    }
}
