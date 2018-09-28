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

import uk.gov.gchq.gaffer.parquetstore.partitioner.GraphPartitioner;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;

public class GraphPartitionerSerialiser {
    private final GroupPartitionerSerialiser groupPartitionerSerialiser = new GroupPartitionerSerialiser();

    public void write(final GraphPartitioner graphPartitioner, final DataOutputStream stream) throws IOException {
        final Set<String> groups = graphPartitioner.getGroups();
        stream.writeInt(groups.size());
        for (final String group : groups) {
            stream.writeUTF(group);
            groupPartitionerSerialiser.write(graphPartitioner.getGroupPartitioner(group), stream);
        }
        final Set<String> groupsForReversedEdges = graphPartitioner.getGroupsForReversedEdges();
        stream.writeInt(groupsForReversedEdges.size());
        for (final String group : groupsForReversedEdges) {
            stream.writeUTF(group);
            groupPartitionerSerialiser.write(graphPartitioner.getGroupPartitionerForReversedEdges(group), stream);
        }
    }

    public GraphPartitioner read(final DataInputStream stream) throws IOException {
        final GraphPartitioner graphPartitioner = new GraphPartitioner();
        int numGroups = stream.readInt();
        for (int i = 0; i < numGroups; i++) {
            final String group = stream.readUTF();
            graphPartitioner.addGroupPartitioner(group, groupPartitionerSerialiser.read(stream));
        }
        numGroups = stream.readInt();
        for (int i = 0; i < numGroups; i++) {
            final String group = stream.readUTF();
            graphPartitioner.addGroupPartitionerForReversedEdges(group, groupPartitionerSerialiser.read(stream));
        }
        return graphPartitioner;
    }
}
