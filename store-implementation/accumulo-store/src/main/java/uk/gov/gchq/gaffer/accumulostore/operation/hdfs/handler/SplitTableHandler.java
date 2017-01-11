/*
 * Copyright 2017 Crown Copyright
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
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.tool.SplitTableTool;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation.SplitTable;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

public class SplitTableHandler implements OperationHandler<SplitTable, Void> {
    @Override
    public Void doOperation(final SplitTable operation,
                            final Context context, final Store store) throws OperationException {
        doOperation(operation, (AccumuloStore) store);
        return null;
    }

    public void doOperation(final SplitTable operation, final AccumuloStore store) throws OperationException {
        splitTable(operation, store);
    }

    private void splitTable(final SplitTable operation, final AccumuloStore store) throws OperationException {
        final SplitTableTool splitTool = new SplitTableTool(operation, store);

        try {
            ToolRunner.run(splitTool, new String[0]);
        } catch (final Exception e) {
            throw new OperationException(e.getMessage(), e);
        }
    }
}
