/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.partitioner;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Copy of {@link  org.apache.accumulo.core.client.mapreduce.lib.partition.KeyRangePartitioner}
 * and swaps the
 * {@link org.apache.accumulo.core.client.mapreduce.lib.partition.RangePartitioner}
 * for the {@link GafferRangePartitioner} to fix a bug with opening the split
 * points file.
 */
public class GafferKeyRangePartitioner extends Partitioner<Key, Writable> implements Configurable {
    private GafferRangePartitioner rp = new GafferRangePartitioner();

    @Override
    public int getPartition(final Key key, final Writable value, final int numPartitions) {
        return rp.getPartition(key.getRow(), value, numPartitions);
    }

    @Override
    public Configuration getConf() {
        return rp.getConf();
    }

    @Override
    public void setConf(final Configuration conf) {
        rp.setConf(conf);
    }

    /**
     * Sets the hdfs file name to use, containing a newline separated list of Base64 encoded split points that represent ranges for partitioning
     *
     * @param job  the job
     * @param file the splits file
     */
    public static void setSplitFile(final Job job, final String file) {
        GafferRangePartitioner.setSplitFile(job, file);
    }

    /**
     * Sets the number of random sub-bins per range
     *
     * @param job the job
     * @param num the number of sub bins
     */
    public static void setNumSubBins(final Job job, final int num) {
        GafferRangePartitioner.setNumSubBins(job, num);
    }
}
