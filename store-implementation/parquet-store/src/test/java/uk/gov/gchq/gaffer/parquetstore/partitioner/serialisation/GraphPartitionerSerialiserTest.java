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

package uk.gov.gchq.gaffer.parquetstore.partitioner.serialisation;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.parquetstore.partitioner.GraphPartitioner;
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

public class GraphPartitionerSerialiserTest {

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Test
    public void shouldGroupMultiplePartitionKeysAndSerialiseCorrectly() throws IOException {
        // Given
        final Object[] key1 = new Object[]{1L, 5, "ABC", 10F, (short) 1, (byte) 64, new byte[]{(byte) 1, (byte) 2, (byte) 3}};
        final PartitionKey partitionKey1 = new PartitionKey(key1);
        final Object[] key2 = new Object[]{100L, 500, "XYZ", 1000F, (short) 3, (byte) 55, new byte[]{(byte) 10, (byte) 9, (byte) 8, (byte) 7}};
        final PartitionKey partitionKey2 = new PartitionKey(key2);
        final List<PartitionKey> splitPoints1 = new ArrayList<>();
        splitPoints1.add(partitionKey1);
        splitPoints1.add(partitionKey2);
        final GroupPartitioner groupPartitioner1 = new GroupPartitioner("GROUP", splitPoints1);
        final Object[] key3 = new Object[]{1000L, 5000, "ABCDEF", 10000F, (short) 19, (byte) 20, new byte[]{(byte) 4, (byte) 5, (byte) 6}};
        final PartitionKey partitionKey3 = new PartitionKey(key3);
        final Object[] key4 = new Object[]{100000L, 500000, "XYZZZZ", 100000F, (short) 32, (byte) 58, new byte[]{(byte) 20, (byte) 29, (byte) 28, (byte) 27}};
        final PartitionKey partitionKey4 = new PartitionKey(key4);
        final List<PartitionKey> splitPoints2 = new ArrayList<>();
        splitPoints2.add(partitionKey3);
        splitPoints2.add(partitionKey4);
        final GroupPartitioner groupPartitioner2 = new GroupPartitioner("GROUP2", splitPoints2);
        final Object[] key5 = new Object[]{10000000L, 5000000, "ABCDEFGHI", 100000F, (short) 21, (byte) 30, new byte[]{(byte) 10, (byte) 11, (byte) 12}};
        final PartitionKey partitionKey5 = new PartitionKey(key5);
        final Object[] key6 = new Object[]{100000000L, 5000, "ABCDEF", 10000F, (short) 19, (byte) 33, new byte[]{(byte) 13, (byte) 14, (byte) 15}};
        final PartitionKey partitionKey6 = new PartitionKey(key6);
        final List<PartitionKey> splitPoints3 = new ArrayList<>();
        splitPoints3.add(partitionKey5);
        splitPoints3.add(partitionKey6);
        final GroupPartitioner groupPartitioner3 = new GroupPartitioner("GROUP1", splitPoints3);
        final GraphPartitioner graphPartitioner = new GraphPartitioner();
        graphPartitioner.addGroupPartitioner("GROUP1", groupPartitioner1);
        graphPartitioner.addGroupPartitioner("GROUP2", groupPartitioner2);
        graphPartitioner.addGroupPartitionerForReversedEdges("GROUP1", groupPartitioner3);
        final GraphPartitionerSerialiser serialiser = new GraphPartitionerSerialiser();

        // When
        final String filename = testFolder.newFolder().getAbsolutePath() + "/test";
        final DataOutputStream dos = new DataOutputStream(new FileOutputStream(filename));
        serialiser.write(graphPartitioner, dos);
        dos.close();
        final DataInputStream dis = new DataInputStream(new FileInputStream(filename));
        final GraphPartitioner readGraphPartitioner = serialiser.read(dis);
        dis.close();

        // Then
        assertEquals(graphPartitioner, readGraphPartitioner);
    }
}
