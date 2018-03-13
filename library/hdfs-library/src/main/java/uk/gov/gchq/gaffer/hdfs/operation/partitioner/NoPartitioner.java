/*
 * Copyright 2016-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.hdfs.operation.partitioner;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * A {@code NoPartioner} can be used to tell a map reduce job not to use a
 * {@link Partitioner}.
 * This class cannot be instantiated.
 */
public final class NoPartitioner extends Partitioner<Void, Void> {
    private NoPartitioner() {
        throw new UnsupportedOperationException("This partitioner should never be instantiated and used");
    }

    @Override
    public int getPartition(final Void key, final Void value, final int numPartitions) {
        throw new UnsupportedOperationException("This partitioner should never be instantiated and used");
    }
}
