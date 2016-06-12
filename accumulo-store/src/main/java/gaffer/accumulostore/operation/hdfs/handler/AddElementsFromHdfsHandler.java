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
import gaffer.accumulostore.operation.hdfs.handler.tool.FetchElementsFromHdfs;
import gaffer.accumulostore.operation.hdfs.handler.tool.ImportElementsToAccumulo;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.operation.OperationException;
import gaffer.operation.simple.hdfs.AddElementsFromHdfs;
import gaffer.store.Context;
import gaffer.store.Store;
import gaffer.store.operation.handler.OperationHandler;
import org.apache.hadoop.util.ToolRunner;

public class AddElementsFromHdfsHandler implements OperationHandler<AddElementsFromHdfs, Void> {
    @Override
    public Void doOperation(final AddElementsFromHdfs operation,
                            final Context context, final Store store)
            throws OperationException {
        doOperation(operation, (AccumuloStore) store);
        return null;
    }

    public void doOperation(final AddElementsFromHdfs operation, final AccumuloStore store) throws OperationException {
        fetchElements(operation, store);
        String skipImport = operation.getOption(AccumuloStoreConstants.ADD_ELEMENTS_FROM_HDFS_SKIP_IMPORT);
        if (null == skipImport || !skipImport.equalsIgnoreCase("TRUE")) {
            importElements(operation, store);
        }
    }

    private void fetchElements(final AddElementsFromHdfs operation, final AccumuloStore store)
            throws OperationException {
        final FetchElementsFromHdfs fetchTool = new FetchElementsFromHdfs(operation, store);

        final int response;
        try {
            response = ToolRunner.run(fetchTool, new String[0]);
        } catch (final Exception e) {
            throw new OperationException("Failed to fetch elements from HDFS", e);
        }

        if (FetchElementsFromHdfs.SUCCESS_RESPONSE != response) {
            throw new OperationException("Failed to fetch elements from HDFS. Response code was: " + response);
        }
    }

    private void importElements(final AddElementsFromHdfs operation, final AccumuloStore store)
            throws OperationException {
        final ImportElementsToAccumulo importTool;
        final int response;
        importTool = new ImportElementsToAccumulo(operation.getOutputPath(), operation.getFailurePath(), store);
        try {
            response = ToolRunner.run(importTool, new String[0]);
        } catch (final Exception e) {
            throw new OperationException("Failed to import elements into Accumulo.", e);
        }

        if (ImportElementsToAccumulo.SUCCESS_RESPONSE != response) {
            throw new OperationException("Failed to import elements into Accumulo. Response code was: " + response);
        }
    }
}
