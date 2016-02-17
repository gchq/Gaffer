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
package gaffer.accumulostore.operation.hdfs.handler.tool;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.operation.hdfs.handler.job.AccumuloAddElementsFromHdfsJobFactory;
import gaffer.accumulostore.utils.TableUtils;
import gaffer.operation.OperationException;
import gaffer.operation.simple.hdfs.AddElementsFromHdfs;

public class FetchElementsFromHdfs extends Configured implements Tool {
    public static final int SUCCESS_RESPONSE = 1;

    private final AddElementsFromHdfs operation;
    private final AccumuloStore store;

    public FetchElementsFromHdfs(final AddElementsFromHdfs operation, final AccumuloStore store) {
        this.operation = operation;
        this.store = store;
    }

    @Override
    public int run(final String[] strings) throws Exception {
        TableUtils.ensureTableExists(store);

        final Job job = new AccumuloAddElementsFromHdfsJobFactory().createJob(operation, store);
        job.waitForCompletion(true);

        if (!job.isSuccessful()) {
            throw new OperationException("Error running job");
        }

        return SUCCESS_RESPONSE;
    }
}
