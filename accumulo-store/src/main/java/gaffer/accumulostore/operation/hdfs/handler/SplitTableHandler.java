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
import gaffer.accumulostore.operation.hdfs.handler.tool.SplitTableTool;
import gaffer.accumulostore.operation.hdfs.impl.SplitTable;
import gaffer.operation.OperationException;
import gaffer.store.Context;
import gaffer.store.Store;
import gaffer.store.operation.handler.OperationHandler;
import org.apache.hadoop.util.ToolRunner;

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
