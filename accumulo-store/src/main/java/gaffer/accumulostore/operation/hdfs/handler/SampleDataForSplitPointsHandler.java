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
package gaffer.accumulostore.operation.hdfs.handler;

import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.operation.hdfs.handler.tool.SampleDataAndCreateSplitsFileTool;
import gaffer.accumulostore.operation.hdfs.impl.SampleDataForSplitPoints;
import gaffer.operation.OperationException;
import gaffer.store.Context;
import gaffer.store.Store;
import gaffer.store.operation.handler.OperationHandler;
import org.apache.hadoop.util.ToolRunner;

public class SampleDataForSplitPointsHandler implements OperationHandler<SampleDataForSplitPoints, String> {
    @Override
    public String doOperation(final SampleDataForSplitPoints operation,
                              final Context context, final Store store)
            throws OperationException {
        return doOperation(operation, (AccumuloStore) store);
    }

    public String doOperation(final SampleDataForSplitPoints operation, final AccumuloStore store) throws OperationException {
        return generateSplitsFromSampleData(operation, store);
    }

    private String generateSplitsFromSampleData(final SampleDataForSplitPoints operation, final AccumuloStore store)
            throws OperationException {
        final SampleDataAndCreateSplitsFileTool sampleTool = new SampleDataAndCreateSplitsFileTool(operation, store);

        try {
            ToolRunner.run(sampleTool, new String[0]);
        } catch (final Exception e) {
            throw new OperationException(e.getMessage(), e);
        }

        return operation.getResultingSplitsFilePath();
    }
}
