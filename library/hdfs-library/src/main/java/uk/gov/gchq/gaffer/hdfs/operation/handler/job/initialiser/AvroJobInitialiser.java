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
package uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import uk.gov.gchq.gaffer.hdfs.operation.MapReduceOperation;
import uk.gov.gchq.gaffer.store.Store;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * An <code>AvroJobInitialiser</code> is an {@link JobInitialiser} that
 * initialises the provided {@link Job} to handle Avro input data.
 */
public class AvroJobInitialiser implements JobInitialiser {
    private String avroSchemaFilePath;

    public AvroJobInitialiser() {
    }

    public AvroJobInitialiser(final String avroSchemaFilePath) {
        this.avroSchemaFilePath = avroSchemaFilePath;
    }

    @Override
    public void initialiseJob(final Job job, final MapReduceOperation operation, final Store store)
            throws IOException {
        initialiseInput(job, operation);
    }

    private void initialiseInput(final Job job, final MapReduceOperation operation) throws IOException {
        if (null == avroSchemaFilePath) {
            throw new IllegalArgumentException("Avro schema file path has not been set");
        }

        final Schema schema = new Parser().parse(new File(avroSchemaFilePath));
        AvroJob.setInputKeySchema(job, schema);
        job.setInputFormatClass(AvroKeyInputFormat.class);
        List<String> paths = operation.getInputPaths();
        for (final String path : paths) {
            AvroKeyInputFormat.addInputPath(job, new Path(path));
        }
    }

    public String getAvroSchemaFilePath() {
        return avroSchemaFilePath;
    }

    public void setAvroSchemaFilePath(final String avroSchemaFilePath) {
        this.avroSchemaFilePath = avroSchemaFilePath;
    }
}
