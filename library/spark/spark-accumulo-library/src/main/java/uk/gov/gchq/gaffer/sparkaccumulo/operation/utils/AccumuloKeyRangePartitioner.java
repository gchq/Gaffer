/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.utils;


import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.apache.spark.Partitioner;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.utils.TableUtils;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.StoreException;

import java.util.Arrays;
import java.util.Collection;

/**
 * This is a spark compatible implementation of the accumulo RangePartitioner ( @link org.apache.accumulo.core.client.mapreduce.lib.partition.RangePartitioner }
 */
public class AccumuloKeyRangePartitioner extends Partitioner {

    private final AccumuloProperties properties;
    private Text[] splits;
    private int numSubBins = 0;

    public AccumuloKeyRangePartitioner(final AccumuloProperties properties) throws OperationException {
        this.properties = properties;
        Collection<Text> splits = getSplits(properties);
        Text[] textArr = new Text[splits.size()];
        this.splits = splits.toArray(textArr);
    }

    private synchronized int getNumSubBins() {
        if (numSubBins < 1) {
            // get number of sub-bins and guarantee it is positive
            numSubBins = Math.max(1, numSubBins == 0 ? 1 : numSubBins);
        }
        return numSubBins;
    }

    private synchronized void setNumSubBins(final int numSubBins) {
        this.numSubBins = numSubBins;
    }

    @Override
    public int numPartitions() {
        return splits.length + 1;
    }

    @Override
    public int getPartition(final Object o) {
        return findPartition(((Key) o).getRow(), getNumSubBins());
    }

    private int findPartition(final Text key, final int numSubBins) {
        // find the bin for the range, and guarantee it is positive
        int index = Arrays.binarySearch(splits, key);
        index = index < 0 ? (index + 1) * -1 : index;

        // both conditions work with numSubBins == 1, but this check is to avoid
        // hashing, when we don't need to, for speed
        if (numSubBins < 2) {
            return index;
        }
        return (key.toString().hashCode() & Integer.MAX_VALUE) % numSubBins + index * numSubBins;
    }

    public void updateSplits() throws OperationException {
        Collection<Text> splits = getSplits(properties);
        Text[] textArr = new Text[splits.size()];
        this.splits = splits.toArray(textArr);
    }

    private static Collection<Text> getSplits(final AccumuloProperties properties) throws OperationException {
        Connector connector = null;
        try {
            connector = TableUtils.getConnector(properties.getInstanceName(), properties.getZookeepers(), properties.getUserName(), properties.getPassword());
        } catch (StoreException e) {
            throw new OperationException("Failed to create accumulo connection", e);
        }
        try {
            return connector.tableOperations().listSplits(properties.getTable());
        } catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
            throw new OperationException("Failed to get accumulo split points from table " + properties.getTable(), e);
        }
    }
}
