/*
 * Copyright 2016-2018 Crown Copyright
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
public final class HBaseUtil {

    private HBaseUtil() {
    }

    public static boolean compareKeys(final Cell left, final Cell right) {
        return HBaseUtil.compareRow(left, right) == 0
                && HBaseUtil.compareFamily(left, right) == 0
                && HBaseUtil.compareQualifier(left, right) == 0
                && HBaseUtil.compareTags(left, right) == 0;
    }

    public static int compareRow(final Cell left, final Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left.getRowArray(), left.getRowOffset(), left.getRowLength(), right.getRowArray(), right.getRowOffset(), right.getRowLength());
    }

    public static int compareFamily(final Cell left, final Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(), right.getFamilyArray(), right.getFamilyOffset(), right.getFamilyLength());
    }

    public static int compareQualifier(final Cell left, final Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left.getQualifierArray(), left.getQualifierOffset(), left.getQualifierLength(), right.getQualifierArray(), right.getQualifierOffset(), right.getQualifierLength());
    }

    public static int compareTags(final Cell left, final Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left.getTagsArray(), left.getTagsOffset(), left.getTagsLength(), right.getTagsArray(), right.getTagsOffset(), right.getTagsLength());
    }
}
