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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.accumulo.core.client.mapreduce.lib.impl.DistributedCacheHelper;
import org.apache.accumulo.core.util.Base64;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.Scanner;
import java.util.TreeSet;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Copy of {@link org.apache.accumulo.core.client.mapreduce.lib.partition.RangePartitioner}
 * but with a fix for opening the cut points file.
 */
@SuppressFBWarnings("IS2_INCONSISTENT_SYNC")
public class GafferRangePartitioner extends Partitioner<Text, Writable> implements Configurable {
    private static final String PREFIX = GafferRangePartitioner.class.getName();
    private static final String CUTFILE_KEY = PREFIX + ".cutFile";
    private static final String NUM_SUBBINS = PREFIX + ".subBins";

    private Configuration conf;

    @Override
    public int getPartition(final Text key, final Writable value, final int numPartitions) {
        try {
            return findPartition(key, getCutPoints(), getNumSubBins());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    int findPartition(final Text key, final Text[] array, final int numSubBins) {
        // find the bin for the range, and guarantee it is positive
        int index = Arrays.binarySearch(array, key);
        index = index < 0 ? (index + 1) * -1 : index;

        // both conditions work with numSubBins == 1, but this check is to avoid
        // hashing, when we don't need to, for speed
        if (numSubBins < 2) {
            return index;
        }
        return (key.toString().hashCode() & Integer.MAX_VALUE) % numSubBins + index * numSubBins;
    }

    private int theNumSubBins = 0;

    private synchronized int getNumSubBins() {
        if (theNumSubBins < 1) {
            // get number of sub-bins and guarantee it is positive
            theNumSubBins = Math.max(1, getConf().getInt(NUM_SUBBINS, 1));
        }
        return theNumSubBins;
    }

    private Text[] cutPointArray = null;

    private synchronized Text[] getCutPoints() throws IOException {
        if (null == cutPointArray) {
            final String cutFileName = conf.get(CUTFILE_KEY);
            final Path[] cf = DistributedCacheHelper.getLocalCacheFiles(conf);

            if (null != cf) {
                for (final Path path : cf) {
                    if (path.toUri().getPath().endsWith(cutFileName.substring(cutFileName.lastIndexOf('/')))) {
                        final TreeSet<Text> cutPoints = new TreeSet<>();
                        try (final Scanner in = openCutPointsStream(path)) {
                            while (in.hasNextLine()) {
                                cutPoints.add(new Text(Base64.decodeBase64(in.nextLine().getBytes(UTF_8))));
                            }
                        }
                        cutPointArray = cutPoints.toArray(new Text[cutPoints.size()]);
                        break;
                    }
                }
            }
            if (null == cutPointArray) {
                throw new FileNotFoundException(cutFileName + " not found in distributed cache");
            }
        }
        return cutPointArray;
    }

    @SuppressFBWarnings("DM_DEFAULT_ENCODING")
    private Scanner openCutPointsStream(final Path path) throws IOException {
        try {
            // Original way of opening the file
            return new Scanner(new BufferedReader(new InputStreamReader(new FileInputStream(path.toString()), UTF_8)));
        } catch (final IOException e) {
            return new Scanner(FileSystem.get(conf).open(path));
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(final Configuration conf) {
        this.conf = conf;
    }

    /**
     * Sets the hdfs file name to use, containing a newline separated list of Base64 encoded split points that represent ranges for partitioning
     *
     * @param job  the job
     * @param file the splits file
     */
    public static void setSplitFile(final Job job, final String file) {
        final URI uri = new Path(file).toUri();
        DistributedCacheHelper.addCacheFile(uri, job.getConfiguration());
        job.getConfiguration().set(CUTFILE_KEY, uri.getPath());
    }

    /**
     * Sets the number of random sub-bins per range
     *
     * @param job the job
     * @param num the number of sub bins
     */
    public static void setNumSubBins(final Job job, final int num) {
        job.getConfiguration().setInt(NUM_SUBBINS, num);
    }
}
