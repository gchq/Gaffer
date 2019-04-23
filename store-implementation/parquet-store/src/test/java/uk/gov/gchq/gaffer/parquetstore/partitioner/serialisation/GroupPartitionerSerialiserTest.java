/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.partitioner.serialisation;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.parquetstore.partitioner.GroupPartitioner;
import uk.gov.gchq.gaffer.parquetstore.partitioner.PartitionKey;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class GroupPartitionerSerialiserTest {

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Test
    public void shouldSerialiseKeysToFileAndReadCorrectly() throws IOException {
        // Given
        final Object[] key1 = new Object[]{1L, 5, "ABC", 10F, (short) 1, (byte) 64, new byte[]{(byte) 1, (byte) 2, (byte) 3}};
        final PartitionKey partitionKey1 = new PartitionKey(key1);
        final Object[] key2 = new Object[]{100L, 500, "XYZ", 1000F, (short) 3, (byte) 55, new byte[]{(byte) 10, (byte) 9, (byte) 8, (byte) 7}};
        final PartitionKey partitionKey2 = new PartitionKey(key2);
        final List<PartitionKey> splitPoints = new ArrayList<>();
        splitPoints.add(partitionKey1);
        splitPoints.add(partitionKey2);
        final GroupPartitioner groupPartitioner = new GroupPartitioner("GROUP", splitPoints);
        final GroupPartitionerSerialiser serialiser = new GroupPartitionerSerialiser();

        // When
        final String filename = testFolder.newFolder().getAbsolutePath() + "/test";
        final DataOutputStream dos = new DataOutputStream(new FileOutputStream(filename));
        serialiser.write(groupPartitioner, dos);
        dos.close();
        final DataInputStream dis = new DataInputStream(new FileInputStream(filename));
        final GroupPartitioner readGroupPartitioner = serialiser.read(dis);
        dis.close();

        // Then
        assertEquals(groupPartitioner, readGroupPartitioner);
    }

    @Test
    public void testWithInfinitePartitionKeys() throws IOException {
        // Given
        final GroupPartitioner groupPartitioner = new GroupPartitioner("GROUP", new ArrayList<>());
        final GroupPartitionerSerialiser serialiser = new GroupPartitionerSerialiser();

        // When
        final String filename = testFolder.newFolder().getAbsolutePath() + "/test";
        final DataOutputStream dos = new DataOutputStream(new FileOutputStream(filename));
        serialiser.write(groupPartitioner, dos);
        dos.close();
        final DataInputStream dis = new DataInputStream(new FileInputStream(filename));
        final GroupPartitioner readGroupPartitioner = serialiser.read(dis);
        dis.close();

        // Then
        assertEquals(readGroupPartitioner, groupPartitioner);
    }
}
