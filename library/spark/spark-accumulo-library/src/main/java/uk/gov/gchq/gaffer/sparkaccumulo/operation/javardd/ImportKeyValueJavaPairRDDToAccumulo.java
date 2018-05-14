/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.javardd;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.spark.api.java.JavaPairRDD;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

@JsonPropertyOrder(value = {"class", "input", "outputPath", "failurePath"}, alphabetic = true)
@Since("1.0.0")
@Summary("Imports key-value Java pair RDD to Accumulo")
public class ImportKeyValueJavaPairRDDToAccumulo implements
        Input<JavaPairRDD<Key, Value>> {
    @Required
    private String outputPath;
    @Required
    private String failurePath;
    private JavaPairRDD<Key, Value> input;
    private Map<String, String> options;

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(final String outputPath) {
        this.outputPath = outputPath;
    }

    public String getFailurePath() {
        return failurePath;
    }

    public void setFailurePath(final String failurePath) {
        this.failurePath = failurePath;
    }

    @Override
    public JavaPairRDD<Key, Value> getInput() {
        return input;
    }

    @Override
    public void setInput(final JavaPairRDD<Key, Value> input) {
        this.input = input;
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
    public ImportKeyValueJavaPairRDDToAccumulo shallowClone() {
        return new ImportKeyValueJavaPairRDDToAccumulo.Builder()
                .outputPath(outputPath)
                .failurePath(failurePath)
                .input(input)
                .options(options)
                .build();
    }

    public static class Builder extends Operation.BaseBuilder<ImportKeyValueJavaPairRDDToAccumulo, Builder>
            implements Input.Builder<ImportKeyValueJavaPairRDDToAccumulo, JavaPairRDD<Key, Value>, Builder> {
        public Builder() {
            super(new ImportKeyValueJavaPairRDDToAccumulo());
        }

        public Builder outputPath(final String outputPath) {
            _getOp().setOutputPath(outputPath);
            return _self();
        }

        public Builder failurePath(final String failurePath) {
            _getOp().setFailurePath(failurePath);
            return _self();
        }
    }
}
