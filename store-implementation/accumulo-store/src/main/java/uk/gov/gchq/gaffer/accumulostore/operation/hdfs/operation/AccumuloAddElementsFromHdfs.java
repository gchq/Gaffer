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

package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation;

import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.MapReduce;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Options;

public abstract class AccumuloAddElementsFromHdfs {

    public static final class Builder extends Operation.BaseBuilder<AddElementsFromHdfs, AccumuloAddElementsFromHdfs.Builder>
            implements AddElementsFromHdfs.IBuilder<AddElementsFromHdfs, AccumuloAddElementsFromHdfs.Builder>,
            MapReduce.Builder<AddElementsFromHdfs, AccumuloAddElementsFromHdfs.Builder>,
            Options.Builder<AddElementsFromHdfs, AccumuloAddElementsFromHdfs.Builder> {
        public Builder() {
            super(new AddElementsFromHdfs());
        }

        public Builder useAccumuloPartitioner(final boolean useAccumuloPartitioner) {
            option(AccumuloStoreConstants.OPERATION_HDFS_USE_ACCUMULO_PARTITIONER, Boolean.toString(useAccumuloPartitioner));
            return _self();
        }

        public Builder useProvidedSplits(final boolean useProvidedSplits) {
            option(AccumuloStoreConstants.OPERATION_HDFS_USE_PROVIDED_SPLITS_FILE, Boolean.toString(useProvidedSplits));
            return _self();
        }

        public Builder splitsFilePath(final String splitsFilePath) {
            option(AccumuloStoreConstants.OPERATION_HDFS_SPLITS_FILE_PATH, splitsFilePath);
            return _self();
        }

        public Builder maxReducers(final int maxReducers) {
            option(AccumuloStoreConstants.OPERATION_BULK_IMPORT_MAX_REDUCERS, Integer.toString(maxReducers));
            return _self();
        }

        public Builder minReducers(final int minReducers) {
            option(AccumuloStoreConstants.OPERATION_BULK_IMPORT_MIN_REDUCERS, Integer.toString(minReducers));
            return _self();
        }

        public Builder skipImport(final boolean skipImport) {
            option(AccumuloStoreConstants.ADD_ELEMENTS_FROM_HDFS_SKIP_IMPORT, Boolean.toString(skipImport));
            return _self();
        }
    }
}
