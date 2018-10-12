/*
 * Copyright 2018. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.partitioner;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A <code>GraphPartitioner</code> specifies the partitioning of the graph across the multiple Parquet files within
 * each group directory.
 */
public class GraphPartitioner {
    private final Map<String, GroupPartitioner> groupToPartitioner;
    private final Map<String, GroupPartitioner> reversedEdgesGroupToPartitioner;

    public GraphPartitioner() {
        this.groupToPartitioner = new HashMap<>();
        this.reversedEdgesGroupToPartitioner = new HashMap<>();
    }

    public void addGroupPartitioner(final String group, final GroupPartitioner groupPartitioner) {
        if (groupToPartitioner.containsKey(group)) {
            throw new IllegalArgumentException("Cannot overwrite the groupPartitioner of a group that already exists in groupToPartitioner (group was "
                    + group + ")");
        }
        groupToPartitioner.put(group, groupPartitioner);
    }

    public void addGroupPartitionerForReversedEdges(final String group, final GroupPartitioner groupPartitioner) {
        if (reversedEdgesGroupToPartitioner.containsKey(group)) {
            throw new IllegalArgumentException("Cannot overwrite the groupPartitioner of a group that already exists in reversedEdgesGroupToPartitioner (group was "
                    + group + ")");
        }
        reversedEdgesGroupToPartitioner.put(group, groupPartitioner);
    }

    public GroupPartitioner getGroupPartitioner(final String group) {
        if (!groupToPartitioner.containsKey(group)) {
            throw new IllegalArgumentException("No GroupPartitioner for key " + group + " exists for groupToPartitioner");
        }
        return groupToPartitioner.get(group);
    }

    public GroupPartitioner getGroupPartitionerForReversedEdges(final String group) {
        if (!reversedEdgesGroupToPartitioner.containsKey(group)) {
            throw new IllegalArgumentException("No GroupPartitioner for key " + group + " exists for reversedEdgesGroupToPartitioner");
        }
        return reversedEdgesGroupToPartitioner.get(group);
    }

    public Set<String> getGroups() {
        return Collections.unmodifiableSet(groupToPartitioner.keySet());
    }

    public Set<String> getGroupsForReversedEdges() {
        return Collections.unmodifiableSet(reversedEdgesGroupToPartitioner.keySet());
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("groupToPartitioner", groupToPartitioner)
                .append("reversedEdgesGroupToPartitioner", reversedEdgesGroupToPartitioner)
                .toString();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final GraphPartitioner other = (GraphPartitioner) obj;

        return new EqualsBuilder()
                .append(groupToPartitioner, other.groupToPartitioner)
                .append(reversedEdgesGroupToPartitioner, other.reversedEdgesGroupToPartitioner)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(groupToPartitioner)
                .append(reversedEdgesGroupToPartitioner)
                .toHashCode();
    }
}
