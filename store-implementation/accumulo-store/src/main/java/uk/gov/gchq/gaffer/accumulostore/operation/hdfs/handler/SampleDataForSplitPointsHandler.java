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
package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler;

import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.factory.AccumuloSampleDataForSplitPointsJobFactory;
import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.tool.SampleDataAndCreateSplitsFileTool;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

public class SampleDataForSplitPointsHandler implements OperationHandler<SampleDataForSplitPoints> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleDataForSplitPointsHandler.class);

    @Override
    public Void doOperation(final SampleDataForSplitPoints operation,
                            final Context context, final Store store)
            throws OperationException {
        generateSplitsFromSampleData(operation, (AccumuloStore) store);
        return null;
    }

    private void generateSplitsFromSampleData(final SampleDataForSplitPoints operation, final AccumuloStore store)
            throws OperationException {
        try {
            if (store.getTabletServers().size() < 2) {
                LOGGER.warn("There is only 1 tablet server so no split points will be calculated.");
                return;
            }
        } catch (final StoreException e) {
            throw new OperationException(e.getMessage(), e);
        }

        final SampleDataAndCreateSplitsFileTool sampleTool = new SampleDataAndCreateSplitsFileTool(new AccumuloSampleDataForSplitPointsJobFactory(), operation, store);
        try {
            ToolRunner.run(sampleTool, new String[0]);
        } catch (final Exception e) {
            throw new OperationException(e.getMessage(), e);
        }

        LOGGER.info("Finished calculating splits");
    }
}
