/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.hbasestore.integration;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.hbasestore.utils.HBaseStoreConstants;
import uk.gov.gchq.gaffer.hdfs.integration.operation.handler.AddElementsFromHdfsIT;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Map;

public class HBaseAddElementsFromHdfsIT extends AddElementsFromHdfsIT {
    @Override
    protected Graph createGraph(final Schema schema) throws Exception {
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graph1")
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .addSchemas(schema)
                .build();
    }

    @Override
    protected AddElementsFromHdfs.Builder createOperation(final String inputDirParam, final Map<String, String> inputMappers) {
        return super.createOperation(inputDirParam, inputMappers)
                .option(HBaseStoreConstants.OPERATION_HDFS_STAGING_PATH, stagingDir);
    }
}
