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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class PartitionKeyTest {

    @Test
    public void testEqualsAndHashcode() {
        // Given
        final PartitionKey partitionKey1 = new PartitionKey(new Object[]{1L, 50, "A"});
        final PartitionKey partitionKey2 = new PartitionKey(new Object[]{1L, 50, "A"});
        final PartitionKey partitionKey3 = new PartitionKey(new Object[]{1L, 50, "B"});

        // When
        final boolean equal = partitionKey1.equals(partitionKey2);
        final boolean notEqual = partitionKey1.equals(partitionKey3);
        final int hashCode1 = partitionKey1.hashCode();
        final int hashCode2 = partitionKey2.hashCode();
        final int hashCode3 = partitionKey3.hashCode();

        // Then
        assertTrue(equal);
        assertFalse(notEqual);
        assertEquals(hashCode1, hashCode2);
        assertNotEquals(hashCode1, hashCode3);
    }

    @Test
    public void testCompare() {
        // Given
        final PartitionKey[] partitionKeys = new PartitionKey[6];
        partitionKeys[0] = new PartitionKey(new Object[]{1L, 50, "A"});
        partitionKeys[1] = new PartitionKey(new Object[]{1L, 50, "B"});
        partitionKeys[2] = new PartitionKey(new Object[]{1L, 500, "A"});
        partitionKeys[3] = new PartitionKey(new Object[]{1L, 500, "B"});
        partitionKeys[4] = new PartitionKey(new Object[]{1L, 5000, "A"});
        partitionKeys[5] = new PartitionKey(new Object[]{10L, 5000, "A"});

        // When / Then
        for (int i = 1; i < partitionKeys.length; i++) {
            assertTrue(partitionKeys[i - 1].compareTo(partitionKeys[i]) < 0);
        }
    }

    @Test
    public void testLengthMethod() {
        // Given
        final PartitionKey partitionKey1 = new PartitionKey(new Object[]{1L, 50, "A"});
        final PartitionKey partitionKey2 = new PartitionKey(new Object[]{1L, 50, "A", "X"});

        // When
        final int partitionKey1Length = partitionKey1.getLength();
        final int partitionKey2Length = partitionKey2.getLength();

        // Then
        assertEquals(3, partitionKey1Length);
        assertEquals(4, partitionKey2Length);
    }
}
