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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GroupPartitionerTest {

    @Test
    public void shouldCorrectlyAssignPartitionIDs() {
        // Given
        final PartitionKey key1 = new PartitionKey(new Object[]{1L, "B"});
        final PartitionKey key2 = new PartitionKey(new Object[]{5L, "A"});
        final PartitionKey key3 = new PartitionKey(new Object[]{100L, "Z"});
        final List<PartitionKey> splitPoints = new ArrayList<>(Arrays.asList(key1, key2, key3));
        final GroupPartitioner partitioner = new GroupPartitioner("GROUP", splitPoints);

        // When
        final int partition1 = partitioner.getPartitionId(new PartitionKey(new Object[]{0L, "Z"}));
        final int partition2 = partitioner.getPartitionId(new PartitionKey(new Object[]{1L, "B"}));
        final int partition3 = partitioner.getPartitionId(new PartitionKey(new Object[]{1L, "C"}));
        final int partition4 = partitioner.getPartitionId(new PartitionKey(new Object[]{5L, "A"}));
        final int partition5 = partitioner.getPartitionId(new PartitionKey(new Object[]{5L, "B"}));
        final int partition6 = partitioner.getPartitionId(new PartitionKey(new Object[]{100L, "Z"}));
        final int partition7 = partitioner.getPartitionId(new PartitionKey(new Object[]{100L, "ZZ"}));

        // Then
        assertEquals(0, partition1);
        assertEquals(1, partition2);
        assertEquals(1, partition3);
        assertEquals(2, partition4);
        assertEquals(2, partition5);
        assertEquals(3, partition6);
        assertEquals(3, partition7);
    }

    @Test
    public void testGetIthPartitionKey() {
        // Given
        final PartitionKey key1 = new PartitionKey(new Object[]{1L, "B"});
        final PartitionKey key2 = new PartitionKey(new Object[]{5L, "A"});
        final PartitionKey key3 = new PartitionKey(new Object[]{100L, "Z"});
        final List<PartitionKey> splitPoints = new ArrayList<>(Arrays.asList(key1, key2, key3));
        final GroupPartitioner partitioner = new GroupPartitioner("GROUP", splitPoints);

        // When
        final PartitionKey zeroth = partitioner.getIthPartitionKey(0);
        final PartitionKey first = partitioner.getIthPartitionKey(1);
        final PartitionKey second = partitioner.getIthPartitionKey(2);

        // Then
        assertEquals(key1, zeroth);
        assertEquals(key2, first);
        assertEquals(key3, second);
    }

    /**
     * e.g. if we have an edge group, the partition keys will consist of source, destination and directed flags. If the
     * data consists of edges 1-2, 1-3, ..., 1-10, 5-4 the partitions might be:
     * Partition 0: 1-2, 1-3
     * Partition 1: 1-4, 1-5, 1-6
     * Partition 2: 1-7, 1-8
     * Partition 3: 1-9
     * Partition 4: 5-4
     * The partition keys would be 1-4, 1-7, 1-9 and 5-4. Some examples of the partition keys for given partial keys:
     * Partial key    Partition ids
     * 0             0
     * 1             0,1,2,3
     * 2             3
     * 6             4
     * 1,2           0
     * 1,4           1
     * 1,6           1
     * 1,7           2
     * 1,10          3
     * 2,10          3
     * 5,3           3
     * 5,4           4
     * 10,10         4
     */
    @Test
    public void testGetPartitionIds() {
        // Given
        final PartitionKey key1 = new PartitionKey(new Object[]{1L, 4L});
        final PartitionKey key2 = new PartitionKey(new Object[]{1L, 7L});
        final PartitionKey key3 = new PartitionKey(new Object[]{1L, 9L});
        final PartitionKey key4 = new PartitionKey(new Object[]{5L, 4L});
        final List<PartitionKey> splitPoints = new ArrayList<>(Arrays.asList(key1, key2, key3, key4));
        final GroupPartitioner partitioner = new GroupPartitioner("GROUP", splitPoints);

        // When
        final List<Integer> partitionIds1 = partitioner.getPartitionIds(new Object[]{0L});
        final List<Integer> partitionIds2 = partitioner.getPartitionIds(new Object[]{1L});
        final List<Integer> partitionIds3 = partitioner.getPartitionIds(new Object[]{2L});
        final List<Integer> partitionIds4 = partitioner.getPartitionIds(new Object[]{6L});
        final List<Integer> partitionIds5 = partitioner.getPartitionIds(new Object[]{1L, 2L});
        final List<Integer> partitionIds6 = partitioner.getPartitionIds(new Object[]{1L, 4L});
        final List<Integer> partitionIds7 = partitioner.getPartitionIds(new Object[]{1L, 6L});
        final List<Integer> partitionIds8 = partitioner.getPartitionIds(new Object[]{1L, 7L});
        final List<Integer> partitionIds9 = partitioner.getPartitionIds(new Object[]{1L, 10L});
        final List<Integer> partitionIds10 = partitioner.getPartitionIds(new Object[]{2L, 10L});
        final List<Integer> partitionIds11 = partitioner.getPartitionIds(new Object[]{5L, 3L});
        final List<Integer> partitionIds12 = partitioner.getPartitionIds(new Object[]{5L, 4L});
        final List<Integer> partitionIds13 = partitioner.getPartitionIds(new Object[]{10L, 10L});

        // Then
        assertEquals(Arrays.asList(0), partitionIds1);
        assertEquals(Arrays.asList(0, 1, 2, 3), partitionIds2);
        assertEquals(Arrays.asList(3), partitionIds3);
        assertEquals(Arrays.asList(4), partitionIds4);
        assertEquals(Arrays.asList(0), partitionIds5);
        assertEquals(Arrays.asList(1), partitionIds6);
        assertEquals(Arrays.asList(1), partitionIds7);
        assertEquals(Arrays.asList(2), partitionIds8);
        assertEquals(Arrays.asList(3), partitionIds9);
        assertEquals(Arrays.asList(3), partitionIds10);
        assertEquals(Arrays.asList(3), partitionIds11);
        assertEquals(Arrays.asList(4), partitionIds12);
        assertEquals(Arrays.asList(4), partitionIds13);
    }

    @Test
    public void testGetPartitionIdsEmptyGroupPartitioner() {
        // Given
        final GroupPartitioner partitioner = new GroupPartitioner("GROUP", new ArrayList<>());

        // When
        final List<Integer> partitionIds = partitioner.getPartitionIds(new Object[]{1L});

        // Then
        assertEquals(Arrays.asList(0), partitionIds);
    }

    @Test
    public void testCannotAddSplitPointsInWrongOrder() {
        // Given
        final PartitionKey key1 = new PartitionKey(new Object[]{1L, "B"});
        final PartitionKey key2 = new PartitionKey(new Object[]{100L, "Z"});
        final PartitionKey key3 = new PartitionKey(new Object[]{5L, "A"});
        final List<PartitionKey> splitPoints = new ArrayList<>(Arrays.asList(key1, key2, key3));

        // When / Then
        try {
            final GroupPartitioner partitioner = new GroupPartitioner("GROUP", splitPoints);
        } catch (final IllegalArgumentException e) {
            // Expected
            return;
        }
        fail("IllegalArgumentException should have been thrown");
    }

    @Test
    public void testCannotAddSplitPointsWithDifferentLengths() {
        // Given
        final PartitionKey key1 = new PartitionKey(new Object[]{1L, "B"});
        final PartitionKey key2 = new PartitionKey(new Object[]{100L, "Z", (short) 2});
        final List<PartitionKey> splitPoints = new ArrayList<>(Arrays.asList(key1, key2));

        // When / Then
        try {
            final GroupPartitioner partitioner = new GroupPartitioner("GROUP", splitPoints);
        } catch (final IllegalArgumentException e) {
            // Expected
            return;
        }
        fail("IllegalArgumentException should have been thrown");
    }

    @Test
    public void testEmptyListIsValid() {
        // Given
        final GroupPartitioner partitioner = new GroupPartitioner("GROUP", new ArrayList<>());

        // When
        final int size = partitioner.getSplitPoints().size();

        // Then
        assertEquals(0, size);
    }

    @Test
    public void testGetPartitionsEmptyList() {
        // Given
        final GroupPartitioner partitioner = new GroupPartitioner("GROUP", new ArrayList<>());

        // When
        final List<Partition> partitions = partitioner.getPartitions();

        // Then
        assertEquals(1, partitions.size());
        assertEquals(new Partition(0, new NegativeInfinityPartitionKey(), new PositiveInfinityPartitionKey()), partitions.get(0));
    }

    @Test
    public void testGetPartitions() {
        // Given
        final PartitionKey key1 = new PartitionKey(new Object[]{1L, "B"});
        final PartitionKey key2 = new PartitionKey(new Object[]{5L, "A"});
        final PartitionKey key3 = new PartitionKey(new Object[]{100L, "Z"});
        final List<PartitionKey> splitPoints = new ArrayList<>(Arrays.asList(key1, key2, key3));
        final GroupPartitioner partitioner = new GroupPartitioner("GROUP", splitPoints);

        // When
        final List<Partition> partitions = partitioner.getPartitions();

        // Then
        assertEquals(4, partitions.size());
        assertEquals(new Partition(0, new NegativeInfinityPartitionKey(), key1), partitions.get(0));
        assertEquals(new Partition(1, key1, key2), partitions.get(1));
        assertEquals(new Partition(2, key2, key3), partitions.get(2));
        assertEquals(new Partition(3, key3, new PositiveInfinityPartitionKey()), partitions.get(3));
    }

    @Test
    public void testEqualsAndHashcode() {
        // Given
        final PartitionKey key1 = new PartitionKey(new Object[]{1L, "B"});
        final PartitionKey key2 = new PartitionKey(new Object[]{5L, "A"});
        final PartitionKey key3 = new PartitionKey(new Object[]{100L, "Z"});
        final List<PartitionKey> splitPoints1 = new ArrayList<>(Arrays.asList(key1, key2, key3));
        final GroupPartitioner partitioner1 = new GroupPartitioner("GROUP", splitPoints1);
        final PartitionKey key4 = new PartitionKey(new Object[]{1L, "B"});
        final PartitionKey key5 = new PartitionKey(new Object[]{5L, "A"});
        final PartitionKey key6 = new PartitionKey(new Object[]{100L, "Z"});
        final List<PartitionKey> splitPoints2 = new ArrayList<>(Arrays.asList(key4, key5, key6));
        final GroupPartitioner partitioner2 = new GroupPartitioner("GROUP", splitPoints2);
        final List<PartitionKey> splitPoints3 = new ArrayList<>(Arrays.asList(key1, key2));
        final GroupPartitioner partitioner3 = new GroupPartitioner("GROUP", splitPoints3);

        // When
        final boolean equal = partitioner1.equals(partitioner2);
        final boolean notEqual = partitioner1.equals(partitioner3);
        final int hashCode1 = partitioner1.hashCode();
        final int hashCode2 = partitioner2.hashCode();
        final int hashCode3 = partitioner3.hashCode();

        // Then
        assertTrue(equal);
        assertFalse(notEqual);
        assertEquals(hashCode1, hashCode2);
        assertNotEquals(hashCode1, hashCode3);
    }
}
