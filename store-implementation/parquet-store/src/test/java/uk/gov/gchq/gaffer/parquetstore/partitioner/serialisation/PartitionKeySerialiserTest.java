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
import uk.gov.gchq.gaffer.parquetstore.partitioner.NegativeInfinityPartitionKey;
import uk.gov.gchq.gaffer.parquetstore.partitioner.PartitionKey;
import uk.gov.gchq.gaffer.parquetstore.partitioner.PositiveInfinityPartitionKey;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class PartitionKeySerialiserTest {

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Test
    public void shouldCreatePartitionKey() throws IOException {
        // Given
        final Object[] key = new Object[]{true, 1L, 5, "ABC", 10F, (short) 1, (byte) 64, new byte[]{(byte) 1, (byte) 2, (byte) 3}};
        final PartitionKey partitionKey = new PartitionKey(key);
        final PartitionKeySerialiser serialiser = new PartitionKeySerialiser();

        // When
        final String filename = testFolder.newFolder().getAbsolutePath() + "/test";
        final DataOutputStream dos = new DataOutputStream(new FileOutputStream(filename));
        serialiser.write(partitionKey, dos);
        dos.close();
        final DataInputStream dis = new DataInputStream(new FileInputStream(filename));
        final PartitionKey readPartitionKey = serialiser.read(dis);
        dis.close();

        // Then
        assertArrayEquals(key, readPartitionKey.getPartitionKey());
    }

    @Test
    public void testWithInfinitePartitionKey() throws IOException {
        // Given
        final PartitionKey negativeInfinity = new NegativeInfinityPartitionKey();
        final PartitionKey positiveInfinity = new PositiveInfinityPartitionKey();
        final PartitionKeySerialiser serialiser = new PartitionKeySerialiser();

        // When
        final String filename = testFolder.newFolder().getAbsolutePath() + "/test";
        final DataOutputStream dos = new DataOutputStream(new FileOutputStream(filename));
        serialiser.write(negativeInfinity, dos);
        serialiser.write(positiveInfinity, dos);
        dos.close();
        final DataInputStream dis = new DataInputStream(new FileInputStream(filename));
        final PartitionKey readPartitionKey1 = serialiser.read(dis);
        final PartitionKey readPartitionKey2 = serialiser.read(dis);
        dis.close();

        // Then
        assertEquals(negativeInfinity, readPartitionKey1);
        assertEquals(positiveInfinity, readPartitionKey2);
    }

    @Test
    public void testEmptyPartitionKey() throws IOException {
        // Given
        final Object[] key = new Object[]{};
        final PartitionKey partitionKey = new PartitionKey(key);
        final PartitionKeySerialiser serialiser = new PartitionKeySerialiser();

        // When
        final String filename = testFolder.newFolder().getAbsolutePath() + "/testEmptyPartitionKey";
        final DataOutputStream dos = new DataOutputStream(new FileOutputStream(filename));
        serialiser.write(partitionKey, dos);
        dos.close();
        final DataInputStream dis = new DataInputStream(new FileInputStream(filename));
        final PartitionKey readPartitionKey = serialiser.read(dis);
        dis.close();

        // Then
        assertArrayEquals(key, readPartitionKey.getPartitionKey());
    }
}
