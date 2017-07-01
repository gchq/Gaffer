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
package uk.gov.gchq.gaffer.flink.operation;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Options;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

public class AddElementsFromFile implements Operation, Options {

    /**
     * The fully qualified path of the file from which Flink should consume
     */
    private String filename;
    /**
     * The name of the job to be created
     */
    private String jobName;
    /**
     * The parallelism of the job to be created
     */
    private int parallelism;
    /**
     * Any properties to be passed to the Flink Job
     */
    private Properties properties;

    private Function<Iterable<? extends String>, Iterable<? extends Element>> elementGenerator;
    private Map<String, String> options;

    public String getFilename() {
        return filename;
    }

    public void setFilename(final String filename) {
        this.filename = filename;
    }

    public String getJobName() {
        return this.jobName;
    }

    public void setJobName(final String jobName) {
        this.jobName = jobName;
    }

    public void setParallelism(final int parallelism) {
        this.parallelism = parallelism;
    }

    public int getParallelism() {
        return this.parallelism;
    }

    public Properties getProperties() {
        return this.properties;
    }

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    public <T extends Function<Iterable<? extends String>, Iterable<? extends Element>> & Serializable> T getElementGenerator() {
        return (T) elementGenerator;
    }

    public <T extends Function<Iterable<? extends String>, Iterable<? extends Element>> & Serializable> void setElementGenerator(final T elementGenerator) {
        this.elementGenerator = elementGenerator;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public static class Builder extends BaseBuilder<AddElementsFromFile, Builder>
            implements Options.Builder<AddElementsFromFile, Builder> {
        public Builder() {
            super(new AddElementsFromFile());
        }

        public <T extends Function<Iterable<? extends String>, Iterable<? extends Element>> & Serializable> Builder generator(final T generator) {
            _getOp().setElementGenerator(generator);
            return _self();
        }

        public Builder filename(final String filename) {
            _getOp().setFilename(filename);
            return _self();
        }

        public Builder jobName(final String jobName) {
            _getOp().setJobName(jobName);
            return _self();
        }

        public Builder parallelism(final int parallelism) {
            _getOp().setParallelism(parallelism);
            return _self();
        }

        public Builder options(final Map<String, String> options) {
            _getOp().setOptions(options);
            return _self();
        }
    }
}
