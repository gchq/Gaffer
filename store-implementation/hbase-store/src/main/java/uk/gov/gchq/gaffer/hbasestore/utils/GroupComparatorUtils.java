/*
 * Copyright 2016-2017 Crown Copyright
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
package uk.gov.gchq.gaffer.hbasestore.utils;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Copied from paulmw - https://github.com/paulmw/hbase-aggregation
 */
public final class GroupComparatorUtils {

    private GroupComparatorUtils() {
    }

    public static boolean compareKeys(final Cell left, final Cell right) {
        return GroupComparatorUtils.compareRow(left, right) == 0
                && GroupComparatorUtils.compareFamily(left, right) == 0
                && GroupComparatorUtils.compareQualifier(left, right) == 0
                && GroupComparatorUtils.compareTags(left, right) == 0;
    }

    public static int compareRow(final byte[] left, final Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left, 0, left.length, right.getRowArray(), right.getRowOffset(), right.getRowLength());
    }

    public static int compareRow(final Cell left, final Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left.getRowArray(), left.getRowOffset(), left.getRowLength(), right.getRowArray(), right.getRowOffset(), right.getRowLength());
    }

    public static int compareFamily(final byte[] left, final Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left, 0, left.length, right.getFamilyArray(), right.getFamilyOffset(), right.getFamilyLength());
    }

    public static int compareFamily(final Cell left, final Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(), right.getFamilyArray(), right.getFamilyOffset(), right.getFamilyLength());
    }

    public static int compareQualifier(final byte[] left, final Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left, 0, left.length, right.getQualifierArray(), right.getQualifierOffset(), right.getQualifierLength());
    }

    public static int compareQualifier(final Cell left, final Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left.getQualifierArray(), left.getQualifierOffset(), left.getQualifierLength(), right.getQualifierArray(), right.getQualifierOffset(), right.getQualifierLength());
    }

    public static int compareTimestamp(final Cell left, final Cell right) {
        // The below older timestamps sorting ahead of newer timestamps looks
        // wrong but it is intentional. This is how HBase stores cells so that
        // the newest cells are found first.
        if (left.getTimestamp() < right.getTimestamp()) {
            return 1;
        } else if (left.getTimestamp() > right.getTimestamp()) {
            return -1;
        }
        return 0;
    }

    public static int compareTags(final byte[] left, final Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left, 0, left.length, right.getTagsArray(), right.getTagsOffset(), right.getTagsLength());
    }

    public static int compareTags(final Cell left, final Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left.getTagsArray(), left.getTagsOffset(), left.getTagsLength(), right.getTagsArray(), right.getTagsOffset(), right.getTagsLength());
    }

    public static int compareTypeByte(final Cell left, final Cell right) {
        return (0xff & left.getTypeByte()) - (0xff & right.getTypeByte());
    }

    public static int compareTypeByte(final byte left, final Cell right) {
        return (0xff & left) - (0xff & right.getTypeByte());
    }
}
