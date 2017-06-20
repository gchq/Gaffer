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
package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.factory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.mapper.SampleDataForSplitPointsMapper;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.reducer.AccumuloKeyValueReducer;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory.AbstractSampleDataForSplitPointsJobFactory;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import java.io.IOException;

public class SampleDataForSplitPointsJobFactory extends AbstractSampleDataForSplitPointsJobFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleDataForSplitPointsJobFactory.class);

    @Override
    public long getOutputEveryNthRecord(final Store store, final long totalNumber) {
        int numberTabletServers;
        try {
            numberTabletServers = ((AccumuloStore) store).getTabletServers().size();
            LOGGER.info("Number of tablet servers is {}", numberTabletServers);
        } catch (final StoreException e) {
            LOGGER.error("Exception thrown getting number of tablet servers: {}", e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }

        return totalNumber / (numberTabletServers - 1);
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "key should always be an instance of Key")
    @Override
    public byte[] createSplit(final Writable key, final Writable value) {
        return ((Key) key).getRow().getBytes();
    }

    @Override
    public Writable createKey() {
        return new Key();
    }

    @Override
    public Writable createValue() {
        return new Value();
    }

    @Override
    public int getExpectedNumberOfSplits(final Store store) {
        int numberTabletServers;
        try {
            numberTabletServers = ((AccumuloStore) store).getTabletServers().size();
            LOGGER.info("Number of tablet servers is {}", numberTabletServers);
        } catch (final StoreException e) {
            LOGGER.error("Exception thrown getting number of tablet servers: {}", e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }

        return numberTabletServers;
    }

    @Override
    protected void setupJobConf(final JobConf jobConf, final SampleDataForSplitPoints operation, final Store store) throws IOException {
        super.setupJobConf(jobConf, operation, store);
        jobConf.set(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS,
                ((AccumuloStore) store).getKeyPackage().getKeyConverter().getClass().getName());
    }

    @Override
    protected void setupJob(final Job job, final SampleDataForSplitPoints operation, final Store store) throws IOException {
        super.setupJob(job, operation, store);
        setupMapper(job, operation, store);
        setupReducer(job, operation, store);
    }

    private void setupMapper(final Job job, final SampleDataForSplitPoints operation, final Store store) throws IOException {
        job.setMapperClass(SampleDataForSplitPointsMapper.class);
        job.setMapOutputKeyClass(Key.class);
        job.setMapOutputValueClass(Value.class);
    }

    private void setupReducer(final Job job, final SampleDataForSplitPoints operation, final Store store)
            throws IOException {
        job.setReducerClass(AccumuloKeyValueReducer.class);
        job.setOutputKeyClass(Key.class);
        job.setOutputValueClass(Value.class);
    }
}
