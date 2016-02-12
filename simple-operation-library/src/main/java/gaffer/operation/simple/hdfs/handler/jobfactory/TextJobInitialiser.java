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
package gaffer.operation.simple.hdfs.handler.jobfactory;

import gaffer.operation.simple.hdfs.AddElementsFromHdfs;
import gaffer.store.Store;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * An <code>AvroJobInitialiser</code> is an {@link gaffer.operation.simple.hdfs.handler.jobfactory.JobInitialiser} that
 * initialises the provided {@link org.apache.hadoop.mapreduce.Job} to handle text input data.
 */
public class TextJobInitialiser implements JobInitialiser {

    public TextJobInitialiser() {
    }

    @Override
    public void initialiseJob(final Job job, final AddElementsFromHdfs operation, final Store store)
            throws IOException {
        initialiseInput(job, operation);
    }

    private void initialiseInput(final Job job, final AddElementsFromHdfs operation) throws IOException {
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, operation.getInputPath());
    }
}
