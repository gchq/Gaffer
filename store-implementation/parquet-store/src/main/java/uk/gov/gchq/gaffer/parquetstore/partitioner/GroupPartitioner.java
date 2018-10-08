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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.parquetstore.utils.SeedComparator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A <code>GroupPartitioner</code> records the list of {@link PartitionKey}s for a given group.
 */
public class GroupPartitioner {

    private final String group;
    private final List<PartitionKey> splitPoints;

    public GroupPartitioner(final String group, final List<PartitionKey> splitPoints) {
        this.group = group;
        checkPartitionKeysAreTheSameLength(splitPoints);
        checkSplitPointsAreOrdered(splitPoints);
        this.splitPoints = splitPoints;
    }

    public String getGroup() {
        return group;
    }

    /**
     * Returns 0 if partitionKey &lt; splitPoints.get(0)
     * Returns 1 if splitPoints.get(0) &le; partitionKey &lt; splitPoints.get(1)
     * ...
     *
     * @param partitionKey the partition key
     * @return the partition id associated to the provided PartitionKey
     */
    public int getPartitionId(final PartitionKey partitionKey) {
        int result = 0;
        for (final PartitionKey key : splitPoints) {
            if (partitionKey.compareTo(key) < 0) {
                return result;
            }
            result++;
        }
        return result;
    }


    /**
     * Returns all the partition ids which could contain data matching the provided partial key.
     *
     * e.g. if we have an edge group, the partition keys will consist of source, destination and directed flags. If the
     * data consists of edges 1-2, 1-3, ..., 1-10, the partitions might be:
     *  Partition 0: 1-2, 1-3
     *  Partition 1: 1-4, 1-5, 1-6
     *  Partition 2: 1-7, 1-8
     *  Partition 3: 1-9
     * The partition keys would be 1-4, 1-7 and 1-9. Some examples of the partition keys for given partial keys:
     *  Partial key    Partition ids
     *     1,2           0
     *     0             0
     *     1             0,1,2,3
     *
     * @param partialKey the partial key
     * @return a list of all the partition ids which could contain data matching the provided partial key
     */
    public List<Integer> getPartitionIds(final Object[] partialKey) {
        if (null == partialKey) {
            throw new IllegalArgumentException("getPartitionIds cannot be called with null partialKey");
        }
        if (0 == splitPoints.size()) {
            return Collections.singletonList(0);
        }
        if (partialKey.length == splitPoints.get(0).getLength()) {
            return getPartitionIdsSameLengthKey(partialKey);
        }
        if (partialKey.length < splitPoints.get(0).getLength()) {
            return getPartitionIdsShorterKey(partialKey);
        }
        throw new IllegalArgumentException("getPartitionIds cannot be called with a partialKey that is longer than the split points");
    }

    private List<Integer> getPartitionIdsSameLengthKey(final Object[] partialKey) {
        final SeedComparator comparator = new SeedComparator();
        final List<Integer> partitionIds = new ArrayList<>();
        for (final Partition partition : getPartitions()) {
            final PartitionKey min = partition.getMin();
            final PartitionKey max = partition.getMax();
            if (min.equals(new NegativeInfinityPartitionKey())) {
                if (comparator.compare(partialKey, max.getPartitionKey()) < 0) {
                    partitionIds.add(partition.getPartitionId());
                }
            } else if (max.equals(new PositiveInfinityPartitionKey())) {
                if (comparator.compare(min.getPartitionKey(), partialKey) <= 0) {
                    partitionIds.add(partition.getPartitionId());
                }
            } else if (comparator.compare(min.getPartitionKey(), partialKey) <= 0 && comparator.compare(partialKey, max.getPartitionKey()) < 0) {
                partitionIds.add(partition.getPartitionId());
            }
        }
        return partitionIds;
    }


    private List<Integer> getPartitionIdsShorterKey(final Object[] partialKey) {
        final SeedComparator comparator = new SeedComparator();
        final List<Integer> partitionIds = new ArrayList<>();
        for (final Partition partition : getPartitions()) {
            final PartitionKey min = partition.getMin();
            final PartitionKey max = partition.getMax();
            if (min.equals(new NegativeInfinityPartitionKey())) {
                if (comparator.compare(partialKey, max.getPartitionKey()) <= 0) {
                    partitionIds.add(partition.getPartitionId());
                }
            } else if (max.equals(new PositiveInfinityPartitionKey())) {
                if (comparator.compare(min.getPartitionKey(), partialKey) <= 0) {
                    partitionIds.add(partition.getPartitionId());
                }
            } else if (comparator.compare(min.getPartitionKey(), partialKey) <= 0 && comparator.compare(partialKey, max.getPartitionKey()) <= 0) {
                partitionIds.add(partition.getPartitionId());
            }
        }
        return partitionIds;
    }


    public PartitionKey getIthPartitionKey(final int i) {
        return splitPoints.get(i);
    }

    public List<PartitionKey> getSplitPoints() {
        return splitPoints;
    }

    // TODO pre-compute this
    public List<Partition> getPartitions() {
        if (splitPoints.isEmpty()) {
            return Collections.singletonList(new Partition(0, new NegativeInfinityPartitionKey(), new PositiveInfinityPartitionKey()));
        }
        final List<Partition> partitions = new ArrayList<>();
        int partitionId = 0;
        PartitionKey previous = new NegativeInfinityPartitionKey();
        for (final PartitionKey splitPoint : splitPoints) {
            partitions.add(new Partition(partitionId, previous, splitPoint));
            partitionId++;
            previous = splitPoint;
        }
        partitions.add(new Partition(partitionId, previous, new PositiveInfinityPartitionKey()));
        return partitions;
    }

    private void checkSplitPointsAreOrdered(final List<PartitionKey> splitPoints) {
        for (int i = 1; i < splitPoints.size(); i++) {
            if (!(splitPoints.get(i - 1).compareTo(splitPoints.get(i)) < 0)) {
                throw new IllegalArgumentException("The splitPoints must be in increasing order: for group " + group + " found the " + (i - 1)
                        + "th split point (" + splitPoints.get(i - 1) + ") was not less than the " + i + "th split point("
                        + splitPoints.get(i) + ")");
            }
        }
    }

    private void checkPartitionKeysAreTheSameLength(final List<PartitionKey> splitPoints) {
        if (splitPoints.isEmpty()) {
            return;
        }
        final Set<Integer> sizes = new HashSet<>();
        for (final PartitionKey partitionKey : splitPoints) {
            sizes.add(partitionKey.getLength());
        }
        if (1 != sizes.size()) {
            throw new IllegalArgumentException("The splitPoints must all be of the same length: found sizes "
                    + StringUtils.join(sizes, ','));
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("group", group)
                .append("splitPoints", splitPoints)
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

        final GroupPartitioner other = (GroupPartitioner) obj;

        return new EqualsBuilder()
                .append(group, other.group)
                .append(splitPoints, other.splitPoints)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(group)
                .append(splitPoints)
                .toHashCode();
    }
}
