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
package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.SplitStore;
import uk.gov.gchq.gaffer.operation.impl.SplitStoreFromFile;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

/**
 * @deprecated use {@link uk.gov.gchq.gaffer.hdfs.operation.handler.HdfsSplitStoreFromFileHandler}.
 */
@Deprecated
public class SplitStoreHandler implements OperationHandler<SplitStore> {
    @Override
    public Void doOperation(final SplitStore operation,
                            final Context context, final Store store) throws OperationException {
        store.execute(
                new SplitStoreFromFile.Builder()
                        .inputPath(operation.getInputPath())
                        .options(operation.getOptions())
                        .build(),
                context
        );
        return null;
    }
}
