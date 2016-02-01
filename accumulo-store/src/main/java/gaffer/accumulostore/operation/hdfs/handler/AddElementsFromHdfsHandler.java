package gaffer.accumulostore.operation.hdfs.handler;

import gaffer.accumulostore.operation.handler.tool.ImportElementsToAccumulo;
import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.operation.hdfs.handler.tool.FetchElementsFromHdfs;
import gaffer.operation.OperationException;
import gaffer.operation.simple.hdfs.AddElementsFromHdfs;
import gaffer.store.Store;
import gaffer.store.StoreException;
import gaffer.store.operation.handler.OperationHandler;
import org.apache.hadoop.util.ToolRunner;

public class AddElementsFromHdfsHandler implements OperationHandler<AddElementsFromHdfs, Void> {
    public Void doOperation(final AddElementsFromHdfs operation, final Store store) throws OperationException {
        doOperation(operation, (AccumuloStore) store);
        return null;
    }

    public void doOperation(final AddElementsFromHdfs operation, AccumuloStore store) throws OperationException {
        fetchElements(operation, store);
        importElements(operation, store);
    }

    private void fetchElements(final AddElementsFromHdfs operation, final AccumuloStore store) throws OperationException {
        final FetchElementsFromHdfs fetchTool = new FetchElementsFromHdfs(operation, store);

        final int response;
        try {
            response = ToolRunner.run(fetchTool, new String[0]);
        } catch (Exception e) {
            throw new OperationException("Failed to fetch elements from HDFS", e);
        }

        if (FetchElementsFromHdfs.SUCCESS_RESPONSE != response) {
            throw new OperationException("Failed to fetch elements from HDFS. Response code was: " + response);
        }
    }

    private void importElements(final AddElementsFromHdfs operation, final AccumuloStore store) throws OperationException {
        final ImportElementsToAccumulo importTool;
        try {
            importTool = new ImportElementsToAccumulo(operation, store);
        } catch (StoreException e) {
            throw new OperationException("Failed to import elements into Accumulo.", e);
        }

        final int response;
        try {
            response = ToolRunner.run(importTool, new String[0]);
        } catch (Exception e) {
            throw new OperationException("Failed to import elements into Accumulo.", e);
        }

        if (ImportElementsToAccumulo.SUCCESS_RESPONSE != response) {
            throw new OperationException("Failed to import elements into Accumulo. Response code was: " + response);
        }
    }
}