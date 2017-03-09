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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler;

import org.apache.hadoop.conf.Configuration;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation.ImportAccumuloKeyValueFiles;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.VoidOutput;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.utils.AccumuloKeyRangePartitioner;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public abstract class AbstractImportKeyValuePairRDDToAccumuloHandler<T extends VoidOutput<?>> implements OperationHandler<T, Void> {

    protected abstract void prepareKeyValues(final T operation, final AccumuloKeyRangePartitioner partitioner) throws OperationException;

    protected abstract String getFailurePath(final T operation);

    protected abstract String getOutputPath(final T operation);

    @Override
    public Void doOperation(final T operation, final Context context, final Store store) throws OperationException {
        doOperation(operation, context, (AccumuloStore) store);
        return null;
    }

    public void doOperation(final T operation, final Context context, final AccumuloStore store) throws OperationException {
        final String outputPath = getOutputPath(operation);
        if (null == outputPath || outputPath.isEmpty()) {
            throw new OperationException("Option outputPath must be set for this option to be run against the accumulostore");
        }
        final String failurePath = getFailurePath(operation);
        if (null == failurePath || failurePath.isEmpty()) {
            throw new OperationException("Option failurePath must be set for this option to be run against the accumulostore");
        }

        prepareKeyValues(operation, new AccumuloKeyRangePartitioner(store));

        final ImportAccumuloKeyValueFiles importAccumuloKeyValueFiles =
                new ImportAccumuloKeyValueFiles.Builder()
                        .inputPath(outputPath)
                        .failurePath(failurePath)
                        .build();
        store._execute(new OperationChain<>(importAccumuloKeyValueFiles), context);
    }

    protected Configuration getConfiguration(final T operation) throws OperationException {
        final Configuration conf = new Configuration();
        final String serialisedConf = operation.getOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY);
        if (serialisedConf != null) {
            try {
                final ByteArrayInputStream bais = new ByteArrayInputStream(serialisedConf.getBytes(CommonConstants.UTF_8));
                conf.readFields(new DataInputStream(bais));
            } catch (final IOException e) {
                throw new OperationException("Exception decoding Configuration from options", e);
            }
        }
        return conf;
    }

}
