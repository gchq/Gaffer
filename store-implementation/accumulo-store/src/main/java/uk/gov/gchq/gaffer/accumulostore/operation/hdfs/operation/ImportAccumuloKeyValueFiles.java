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

package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.koryphe.Since;

import java.util.Map;

@JsonPropertyOrder(value = {"class", "inputPath", "failurePath"}, alphabetic = true)
@Since("1.0.0")
public class ImportAccumuloKeyValueFiles implements
        Operation {
    @Required
    private String failurePath;
    @Required
    private String inputPath;
    private Map<String, String> options;

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

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public ImportAccumuloKeyValueFiles shallowClone() {
        return new ImportAccumuloKeyValueFiles.Builder()
                .failurePath(failurePath)
                .inputPath(inputPath)
                .options(options)
                .build();
    }


    public static class Builder extends Operation.BaseBuilder<ImportAccumuloKeyValueFiles, Builder> {
        public Builder() {
            super(new ImportAccumuloKeyValueFiles());
        }

        public Builder inputPath(final String inputPath) {
            _getOp().setInputPath(inputPath);
            return _self();
        }

        public Builder failurePath(final String failurePath) {
            _getOp().setFailurePath(failurePath);
            return _self();
        }
    }
}
