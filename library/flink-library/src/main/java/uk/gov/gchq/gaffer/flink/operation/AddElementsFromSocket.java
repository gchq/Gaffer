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

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Options;
import uk.gov.gchq.gaffer.operation.Validatable;
import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;

public class AddElementsFromSocket implements
        Operation,
        Validatable,
        Options {
    public static final String DEFAULT_DELIMITER = "\n";

    @Required
    private String hostname;
    @Required
    private int port;
    @Required
    private Function<Iterable<? extends String>, Iterable<? extends Element>> elementGenerator;
    @Required
    private String jobName;
    private Map<String, String> options;
    private boolean validate = true;
    private boolean skipInvalidElements;
    private String delimiter = DEFAULT_DELIMITER;

    public String getHostname() {
        return hostname;
    }

    public void setHostname(final String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(final int port) {
        this.port = port;
    }

    public String getJobName() {
        return this.jobName;
    }

    public void setJobName(final String jobName) {
        this.jobName = jobName;
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

    @Override
    public boolean isSkipInvalidElements() {
        return skipInvalidElements;
    }

    @Override
    public void setSkipInvalidElements(final boolean skipInvalidElements) {
        this.skipInvalidElements = skipInvalidElements;
    }

    @Override
    public boolean isValidate() {
        return validate;
    }

    @Override
    public void setValidate(final boolean validate) {
        this.validate = validate;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(final String delimiter) {
        this.delimiter = delimiter;
    }

    public static class Builder extends Operation.BaseBuilder<AddElementsFromSocket, Builder>
            implements Validatable.Builder<AddElementsFromSocket, Builder>,
            Options.Builder<AddElementsFromSocket, Builder> {
        public Builder() {
            super(new AddElementsFromSocket());
        }

        public <T extends Function<Iterable<? extends String>, Iterable<? extends Element>> & Serializable> Builder generator(final T generator) {
            _getOp().setElementGenerator(generator);
            return _self();
        }

        public Builder hostname(final String hostname) {
            _getOp().setHostname(hostname);
            return _self();
        }

        public Builder port(final int port) {
            _getOp().setPort(port);
            return _self();
        }

        public Builder jobName(final String jobName) {
            _getOp().setJobName(jobName);
            return _self();
        }

        public Builder delimiter(final String delimiter) {
            _getOp().setDelimiter(delimiter);
            return _self();
        }
    }
}
