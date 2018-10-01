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

import uk.gov.gchq.gaffer.parquetstore.partitioner.GroupPartitioner;
import uk.gov.gchq.gaffer.parquetstore.partitioner.PartitionKey;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GroupPartitionerSerialiser {
    private final PartitionKeySerialiser partitionKeySerialiser = new PartitionKeySerialiser();

    public void write(final GroupPartitioner groupPartitioner, final DataOutputStream stream) throws IOException {
        stream.writeUTF(groupPartitioner.getGroup());
        stream.writeInt(groupPartitioner.getSplitPoints().size());
        for (final PartitionKey partitionKey : groupPartitioner.getSplitPoints()) {
            partitionKeySerialiser.write(partitionKey, stream);
        }
    }

    public GroupPartitioner read(final DataInputStream stream) throws IOException {
        final String group = stream.readUTF();
        final int numberOfSplitPoints = stream.readInt();
        final List<PartitionKey> splitPoints = new ArrayList<>();
        for (int i = 0; i < numberOfSplitPoints; i++) {
            splitPoints.add(partitionKeySerialiser.read(stream));
        }
        return new GroupPartitioner(group, splitPoints);
    }
}
