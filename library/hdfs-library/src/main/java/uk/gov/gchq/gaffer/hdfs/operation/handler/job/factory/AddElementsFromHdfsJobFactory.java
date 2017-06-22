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
package uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory;

import org.apache.hadoop.mapreduce.Job;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import java.io.IOException;


public interface AddElementsFromHdfsJobFactory extends JobFactory<AddElementsFromHdfs> {
    /**
     * Creates a job with the store specific job initialisation and then applies the operation specific
     * {@link uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser.JobInitialiser}.
     *
     * @param operation the add elements from hdfs operation
     * @param store     the store executing the operation
     * @return the created job
     * @throws IOException for IO issues
     */
    @Override
    Job createJob(final AddElementsFromHdfs operation, final Store store) throws IOException;

    /**
     * Prepares the store for the add from hdfs.
     * For example this could create a table to store the elements in.
     *
     * @param store the store
     * @throws StoreException if an error occurs
     */
    void prepareStore(final Store store) throws StoreException;
}
