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
import org.apache.hadoop.fs.Path;

public class ImportAccumuloKeyValueFiles extends AbstractOperation<Path, Void> implements VoidOutput<Path> {

    private Path failurePath;
    private Path inputPath;

    public ImportAccumuloKeyValueFiles() {

    }

    public Path getInputPath() {
        return inputPath;
    }

    public void setInputPath(final Path inputPath) {
        this.inputPath = inputPath;
    }

    public Path getFailurePath() {
        return failurePath;
    }

    public void setFailurePath(final Path failurePath) {
        this.failurePath = failurePath;
    }

    public static class Builder extends AbstractOperation.Builder<ImportAccumuloKeyValueFiles, Path, Void> {
        public Builder() {
            super(new ImportAccumuloKeyValueFiles());
        }

        public Builder inputPath(final Path inputPath) {
            op.setInputPath(inputPath);
            return this;
        }

        @Override
        public Builder option(final String name, final String value) {
            super.option(name, value);
            return this;
        }

        public Builder failurePath(final Path failurePath) {
            op.setFailurePath(failurePath);
            return this;
        }
    }
}
