/*
 * Copyright 2016-2017 Crown Copyright
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
package uk.gov.gchq.gaffer.hbasestore.operation.hdfs.handler.job.factory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.operation.hdfs.mapper.AddElementsFromHdfsMapper;
import uk.gov.gchq.gaffer.hbasestore.utils.HBaseStoreConstants;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory.AbstractAddElementsFromHdfsJobFactory;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import java.io.IOException;

public class HBaseAddElementsFromHdfsJobFactory extends
        AbstractAddElementsFromHdfsJobFactory {
    @Override
    protected JobConf createJobConf(final AddElementsFromHdfs operation, final Store store) throws IOException {
        return new JobConf(((HBaseStore) store).getConfiguration());
    }

    @Override
    public void setupJob(final Job job, final AddElementsFromHdfs operation, final Store store) throws IOException {
        super.setupJob(job, operation, store);

        setupMapper(job);
        setupReducer(job);
        setupOutput(job, operation, (HBaseStore) store);
    }

    private void setupMapper(final Job job) throws IOException {
        job.setMapperClass(AddElementsFromHdfsMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
    }

    private void setupReducer(final Job job) throws IOException {
        job.setReducerClass(PutSortReducer.class);
    }

    private void setupOutput(final Job job, final AddElementsFromHdfs operation, final HBaseStore store) throws IOException {
        FileOutputFormat.setOutputPath(job, new Path(operation.getOutputPath()));
        final String stagingDir = operation.getOption(HBaseStoreConstants.OPERATION_HDFS_STAGING_PATH);
        if (null != stagingDir && !stagingDir.isEmpty()) {
            job.getConfiguration().set(HConstants.TEMPORARY_FS_DIRECTORY_KEY, stagingDir);
        }

        try {
            HFileOutputFormat2.configureIncrementalLoad(
                    job,
                    store.getTable(),
                    store.getConnection().getRegionLocator(store.getProperties().getTable())
            );
        } catch (final StoreException e) {
            throw new RuntimeException(e);
        }
    }
}

