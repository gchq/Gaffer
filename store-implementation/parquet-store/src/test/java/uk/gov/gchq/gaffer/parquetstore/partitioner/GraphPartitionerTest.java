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

public class GraphPartitionerTest {

    @Test
    public void test() {
        // Given
        final String group1 = "GROUP1";
        final PartitionKey key1 = new PartitionKey(new Object[]{1L, "B"});
        final PartitionKey key2 = new PartitionKey(new Object[]{5L, "A"});
        final PartitionKey key3 = new PartitionKey(new Object[]{100L, "Z"});
        final List<PartitionKey> splitPoints1 = new ArrayList<>(Arrays.asList(key1, key2, key3));
        final GroupPartitioner partitioner1 = new GroupPartitioner(group1, splitPoints1);
        final String group2 = "GROUP2";
        final PartitionKey key4 = new PartitionKey(new Object[]{"A", "C", 1000L});
        final PartitionKey key5 = new PartitionKey(new Object[]{"A", "Z", 1L});
        final PartitionKey key6 = new PartitionKey(new Object[]{"T", "B", 500L});
        final List<PartitionKey> splitPoints2 = new ArrayList<>(Arrays.asList(key4, key5, key6));
        final GroupPartitioner partitioner2 = new GroupPartitioner(group2, splitPoints2);
        final GraphPartitioner graphPartitioner = new GraphPartitioner();
        graphPartitioner.addGroupPartitioner(group1, partitioner1);
        graphPartitioner.addGroupPartitionerForReversedEdges(group2, partitioner2);

        // When
        final int partition1 = graphPartitioner
                .getGroupPartitioner(group1).getPartitionId(new PartitionKey(new Object[]{0L, "Z"}));
        final int partition2 = graphPartitioner
                .getGroupPartitioner(group1).getPartitionId(new PartitionKey(new Object[]{1L, "B"}));
        final int partition3 = graphPartitioner
                .getGroupPartitioner(group1).getPartitionId(new PartitionKey(new Object[]{1L, "C"}));
        final int partition4 = graphPartitioner
                .getGroupPartitioner(group1).getPartitionId(new PartitionKey(new Object[]{5L, "A"}));
        final int partition5 = graphPartitioner
                .getGroupPartitioner(group1).getPartitionId(new PartitionKey(new Object[]{5L, "B"}));
        final int partition6 = graphPartitioner
                .getGroupPartitioner(group1).getPartitionId(new PartitionKey(new Object[]{100L, "Z"}));
        final int partition7 = graphPartitioner
                .getGroupPartitioner(group1).getPartitionId(new PartitionKey(new Object[]{100L, "ZZ"}));
        final int partition8 = graphPartitioner
                .getGroupPartitionerForReversedEdges(group2).getPartitionId(new PartitionKey(new Object[]{"A", "B", 10L}));
        final int partition9 = graphPartitioner
                .getGroupPartitionerForReversedEdges(group2).getPartitionId(new PartitionKey(new Object[]{"A", "C", 1000L}));
        final int partition10 = graphPartitioner
                .getGroupPartitionerForReversedEdges(group2).getPartitionId(new PartitionKey(new Object[]{"A", "D", 10L}));
        final int partition11 = graphPartitioner
                .getGroupPartitionerForReversedEdges(group2).getPartitionId(new PartitionKey(new Object[]{"A", "Z", 1L}));
        final int partition12 = graphPartitioner
                .getGroupPartitionerForReversedEdges(group2).getPartitionId(new PartitionKey(new Object[]{"A", "Z", 12L}));
        final int partition13 = graphPartitioner
                .getGroupPartitionerForReversedEdges(group2).getPartitionId(new PartitionKey(new Object[]{"T", "B", 500L}));
        final int partition14 = graphPartitioner
                .getGroupPartitionerForReversedEdges(group2).getPartitionId(new PartitionKey(new Object[]{"T", "Z", 10L}));
        final int partition15 = graphPartitioner
                .getGroupPartitionerForReversedEdges(group2).getPartitionId(new PartitionKey(new Object[]{"Z", "Z", 10L}));

        // Then
        assertEquals(0, partition1);
        assertEquals(1, partition2);
        assertEquals(1, partition3);
        assertEquals(2, partition4);
        assertEquals(2, partition5);
        assertEquals(3, partition6);
        assertEquals(3, partition7);
        assertEquals(0, partition8);
        assertEquals(1, partition9);
        assertEquals(1, partition10);
        assertEquals(2, partition11);
        assertEquals(2, partition12);
        assertEquals(3, partition13);
        assertEquals(3, partition14);
        assertEquals(3, partition15);
    }

    @Test
    public void testCannotOverwriteExistingGroupPartitioner() {
        // Given
        final String group = "GROUP1";
        final PartitionKey key1 = new PartitionKey(new Object[]{1L, "B"});
        final PartitionKey key2 = new PartitionKey(new Object[]{5L, "A"});
        final PartitionKey key3 = new PartitionKey(new Object[]{100L, "Z"});
        final List<PartitionKey> splitPoints1 = new ArrayList<>(Arrays.asList(key1, key2, key3));
        final GroupPartitioner partitioner1 = new GroupPartitioner(group, splitPoints1);
        final GraphPartitioner graphPartitioner = new GraphPartitioner();
        graphPartitioner.addGroupPartitioner(group, partitioner1);

        // When / Then
        final PartitionKey key4 = new PartitionKey(new Object[]{"A", "C", 1000L});
        final PartitionKey key5 = new PartitionKey(new Object[]{"A", "Z", 1L});
        final PartitionKey key6 = new PartitionKey(new Object[]{"T", "B", 500L});
        final List<PartitionKey> splitPoints2 = new ArrayList<>(Arrays.asList(key4, key5, key6));
        final GroupPartitioner partitioner2 = new GroupPartitioner(group, splitPoints2);
        try {
            graphPartitioner.addGroupPartitioner(group, partitioner2);
        } catch (final IllegalArgumentException e) {
            // Expected
            return;
        }
        fail("IllegalArgumentException should have been thrown");
    }

    @Test
    public void testReturnsUsefulExceptionIfAskForPartitionerForNonExistentGroup() {
        // Given
        final GraphPartitioner graphPartitioner = new GraphPartitioner();

        // When / Then
        try {
            final GroupPartitioner partitioner = graphPartitioner.getGroupPartitioner("NOT_THERE");
        } catch (final IllegalArgumentException e) {
            // Expected
            return;
        }
        fail("IllegalArgumentException should have been thrown");
    }

    @Test
    public void testReturnsUsefulExceptionIfAskForPartitionerForNonExistentGroupForReversedEdges() {
        // Given
        final GraphPartitioner graphPartitioner = new GraphPartitioner();

        // When / Then
        try {
            final GroupPartitioner partitioner = graphPartitioner.getGroupPartitionerForReversedEdges("NOT_THERE");
        } catch (final IllegalArgumentException e) {
            // Expected
            return;
        }
        fail("IllegalArgumentException should have been thrown");
    }

    private GraphPartitioner getGraphPartitioner() {
        final String group1 = "GROUP1";
        final PartitionKey key1 = new PartitionKey(new Object[]{1L, "B"});
        final PartitionKey key2 = new PartitionKey(new Object[]{5L, "A"});
        final PartitionKey key3 = new PartitionKey(new Object[]{100L, "Z"});
        final List<PartitionKey> splitPoints1 = new ArrayList<>(Arrays.asList(key1, key2, key3));
        final GroupPartitioner partitioner1 = new GroupPartitioner(group1, splitPoints1);
        final String group2 = "GROUP2";
        final PartitionKey key4 = new PartitionKey(new Object[]{"A", "C", 1000L});
        final PartitionKey key5 = new PartitionKey(new Object[]{"A", "Z", 1L});
        final PartitionKey key6 = new PartitionKey(new Object[]{"T", "B", 500L});
        final List<PartitionKey> splitPoints2 = new ArrayList<>(Arrays.asList(key4, key5, key6));
        final GroupPartitioner partitioner2 = new GroupPartitioner(group2, splitPoints2);
        final GraphPartitioner graphPartitioner = new GraphPartitioner();
        graphPartitioner.addGroupPartitioner(group1, partitioner1);
        graphPartitioner.addGroupPartitionerForReversedEdges(group2, partitioner2);
        return graphPartitioner;
    }

    @Test
    public void testEqualsAndHashcode() {
        // Given
        final GraphPartitioner graphPartitioner1 = getGraphPartitioner();
        final GraphPartitioner graphPartitioner2 = getGraphPartitioner();
        final GraphPartitioner graphPartitioner3 = getGraphPartitioner();
        final PartitionKey key = new PartitionKey(new Object[]{1, 2, 3});
        final GroupPartitioner partitioner = new GroupPartitioner("GROUP3", new ArrayList<>(Arrays.asList(key)));
        graphPartitioner3.addGroupPartitioner("GROUP3", partitioner);

        // When
        final boolean equal = graphPartitioner1.equals(graphPartitioner2);
        final boolean notEqual = graphPartitioner1.equals(graphPartitioner3);
        final int hashCode1 = graphPartitioner1.hashCode();
        final int hashCode2 = graphPartitioner2.hashCode();
        final int hashCode3 = graphPartitioner3.hashCode();

        // Then
        assertTrue(equal);
        assertFalse(notEqual);
        assertEquals(hashCode1, hashCode2);
        assertNotEquals(hashCode1, hashCode3);
    }
}
