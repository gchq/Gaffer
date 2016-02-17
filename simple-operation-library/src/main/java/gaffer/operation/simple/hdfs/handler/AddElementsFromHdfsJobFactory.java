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
package gaffer.operation.simple.hdfs.handler;

import gaffer.operation.simple.hdfs.AddElementsFromHdfs;
import gaffer.store.Store;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public interface AddElementsFromHdfsJobFactory {
    String UTF_8_CHARSET = "UTF-8";
    String DATA_SCHEMA = "dataSchema";
    String STORE_SCHEMA = "storeSchema";
    String MAPPER_GENERATOR = "mapperGenerator";
    String VALIDATE = "validate";

    Job createJob(final AddElementsFromHdfs operation, final Store store) throws IOException;
}
