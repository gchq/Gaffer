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
import uk.gov.gchq.gaffer.store.StoreException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to store a file-based groupToIndex for each group, i.e. for each group it stores a {@link GroupIndex}
 * which is a set of {@link ColumnIndex}s. Each {@link ColumnIndex} contains a path to a file that contains data for
 * that group along with minimum and maximum values of the indexed columns within that file. This allows queries for
 * particular values of the indexed columns to skip files that do not contain relevant data.
 */
public class GraphIndex {
    private final Map<String, GroupIndex> groupToIndex;
    private long snapshot;

    public GraphIndex() {
        this.groupToIndex = new HashMap<>();
    }

    public void add(final String group, final GroupIndex groupIndex) {
        if (groupToIndex.containsKey(group)) {
            throw new IllegalArgumentException("Cannot overwrite an entry in an groupToIndex (group was " + group + ")");
        }
        groupToIndex.put(group, groupIndex);
    }

    public Set<String> groupsIndexed() {
        return Collections.unmodifiableSet(groupToIndex.keySet());
    }

    public GroupIndex getGroup(final String group) {
        return groupToIndex.get(group);
    }

    public long getSnapshotTimestamp() {
        return snapshot;
    }

    public void setSnapshotTimestamp(final long snapshotTimestamp) {
        snapshot = snapshotTimestamp;
    }

    public void writeGroups(final String rootDir, final FileSystem fs) throws StoreException {
        for (final Map.Entry<String, GroupIndex> groupIndexEntry : groupToIndex.entrySet()) {
            final String group = groupIndexEntry.getKey();
            final GroupIndex groupIndex = groupIndexEntry.getValue();
            groupIndex.writeColumns(group, rootDir, fs);
        }
    }

    public void readGroups(final Set<String> groups, final String rootDir, final FileSystem fs) throws StoreException {
        for (final String group : groups) {
            final GroupIndex groupIndex = new GroupIndex();
            groupIndex.readColumns(group, rootDir, fs);
            add(group, groupIndex);
        }
    }
}
