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

package gaffer.accumulostore.operation.hdfs.impl;

import gaffer.operation.AbstractOperation;
import gaffer.operation.VoidOutput;

public class ImportAccumuloKeyValueFiles extends AbstractOperation<String, Void> implements VoidOutput<String> {

    private String failurePath;
    private String inputPath;

    public ImportAccumuloKeyValueFiles() {

    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(final String inputPath) {
        this.inputPath = inputPath;
    }

    public String getFailurePath() {
        return failurePath;
    }

    public void setFailurePath(final String failurePath) {
        this.failurePath = failurePath;
    }

    public static class Builder extends AbstractOperation.Builder<ImportAccumuloKeyValueFiles, String, Void> {
        public Builder() {
            super(new ImportAccumuloKeyValueFiles());
        }

        public Builder inputPath(final String inputPath) {
            op.setInputPath(inputPath);
            return this;
        }

        @Override
        public Builder option(final String name, final String value) {
            super.option(name, value);
            return this;
        }

        public Builder failurePath(final String failurePath) {
            op.setFailurePath(failurePath);
            return this;
        }
    }
}
