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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.parquet.bytes.BytesUtils;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.store.StoreException;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * This class is used to store a file-based index for a single gaffer column, i.e. it stores a set of
 * {@link MinValuesWithPath}s. Each {@link MinValuesWithPath} contains a path to a file that has been sorted by the column this index
 * is for and contains the min and max parquet objects from that parquet file.
 */
public class ColumnIndex {
    private static final Comparator<MinValuesWithPath> BY_PATH =
            (MinValuesWithPath mmp1, MinValuesWithPath mmp2) -> mmp1.getPath().compareTo(mmp2.getPath());
    private final SortedSet<MinValuesWithPath> minValuesWithPaths;

    public ColumnIndex() {
        this.minValuesWithPaths = new TreeSet<>(BY_PATH);
    }

    public boolean isEmpty() {
        return minValuesWithPaths.isEmpty();
    }

    public void add(final MinValuesWithPath minValuesWithPath) {
        minValuesWithPaths.add(minValuesWithPath);
    }

    public Iterator<MinValuesWithPath> getIterator() {
            return minValuesWithPaths.iterator();
        }

    protected void write(final FSDataOutputStream outputFile) throws StoreException {
        try {
            for (final MinValuesWithPath minValuesWithPath : minValuesWithPaths) {
                final Object[] min = minValuesWithPath.getMin();
                final String path = minValuesWithPath.getPath();
                outputFile.writeInt(min.length);
                for (int colIndex = 0; colIndex < min.length; colIndex++) {
                    final Object minCol = min[colIndex];
                    final String minColType = minCol.getClass().getSimpleName();
                    final byte[] columnType = StringUtil.toBytes(minColType);
                    outputFile.writeInt(columnType.length);
                    outputFile.write(columnType);
                    final byte[] minValue = serialiseObject(min[colIndex]);
                    outputFile.writeInt(minValue.length);
                    outputFile.write(minValue);
                }
                byte[] filePath = StringUtil.toBytes(path);
                outputFile.writeInt(filePath.length);
                outputFile.write(filePath);
                outputFile.hsync();
            }
            outputFile.hsync();
            outputFile.close();
        } catch (final IOException e) {
            throw new StoreException(e.getMessage());
        }
    }

    private byte[] serialiseObject(final Object value) throws StoreException {
        final String objectType = value.getClass().getSimpleName();
        if ("Long".equals(objectType)) {
            return BytesUtils.longToBytes((long) value);
        } else if ("Integer".equals(objectType)) {
            return BytesUtils.intToBytes((int) value);
        } else if ("Boolean".equals(objectType)) {
            return BytesUtils.booleanToBytes((boolean) value);
        } else if ("Float".equals(objectType)) {
            return BytesUtils.intToBytes(Float.floatToIntBits((float) value));
        } else if ("String".equals(objectType)) {
            return StringUtil.toBytes((String) value);
        } else if ("byte[]".equals(objectType)) {
            return (byte[]) value;
        } else {
            throw new StoreException("Cannot serialise objects of type " + objectType);
        }
    }

    public void read(final FSDataInputStream reader) throws StoreException {
        try {
            while (reader.available() > 0) {
                final int numOfCols = reader.readInt();
                Object[] min = new Object[numOfCols];
                for (int i = 0; i < numOfCols; i++) {
                    final int colTypeLength = reader.readInt();
                    final byte[] colType = readBytes(colTypeLength, reader);
                    final int minLength = reader.readInt();
                    final byte[] minBytes = readBytes(minLength, reader);
                    min[i] = deserialiseColumn(colType, minBytes);
                }
                final int filePathLength = reader.readInt();
                final byte[] filePath = readBytes(filePathLength, reader);
                add(new MinValuesWithPath(min, StringUtil.toString(filePath)));
            }
            reader.close();
        } catch (final IOException e) {
            throw new StoreException(e.getMessage());
        }
    }

    private byte[] readBytes(final int length, final FSDataInputStream reader) throws StoreException {
        try {
            final byte[] bytes = new byte[length];
            int bytesRead = 0;
            while (bytesRead < length && bytesRead > -1) {
                bytesRead += reader.read(bytes, bytesRead, length - bytesRead);
            }
            return bytes;
        } catch (final IOException e) {
            throw new StoreException(e.getMessage());
        }
    }

    private Object deserialiseColumn(final byte[] colType, final byte[] value) {
        final String colTypeName = StringUtil.toString(colType);
        if ("Long".equals(colTypeName)) {
            return BytesUtils.bytesToLong(value);
        } else if ("Integer".equals(colTypeName)) {
            return BytesUtils.bytesToInt(value);
        } else if ("Boolean".equals(colTypeName)) {
            return BytesUtils.bytesToBool(value);
        } else if ("Float".equals(colTypeName)) {
            return Float.intBitsToFloat(BytesUtils.bytesToInt(value));
        } else if ("String".equals(colTypeName)) {
            return StringUtil.toString(value);
        } else {
            return value;
        }
    }
}
