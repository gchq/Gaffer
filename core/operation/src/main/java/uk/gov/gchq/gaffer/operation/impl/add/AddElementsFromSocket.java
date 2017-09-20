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
package uk.gov.gchq.gaffer.operation.impl.add;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Validatable;

import java.util.Map;
import java.util.function.Function;

/**
 * An {@code AddElementsFromSocket} operation consumes records from a socket,
 * converts each record into a Gaffer {@link Element} using the provided
 * {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} then adds these
 * elements to the Graph. This operation uses Flink so you can either run it
 * in local mode or configure flink on your cluster to distribute the job.
 * This operation is a blocking operation and will only stop when the socket is
 * closed or you manually terminate the job.
 *
 * @see Builder
 */
public class AddElementsFromSocket implements
        Operation,
        Validatable {
    public static final String DEFAULT_DELIMITER = "\n";

    @Required
    private String hostname;

    @Required
    private int port;

    @Required
    private Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> elementGenerator;

    /**
     * The parallelism of the job to be created
     */
    private Integer parallelism;

    private boolean validate = true;
    private boolean skipInvalidElements;
    private String delimiter = DEFAULT_DELIMITER;
    private Map<String, String> options;

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

    public Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> getElementGenerator() {
        return elementGenerator;
    }

    public void setElementGenerator(final Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> elementGenerator) {
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

    public void setParallelism(final Integer parallelism) {
        this.parallelism = parallelism;
    }

    public Integer getParallelism() {
        return this.parallelism;
    }

    @Override
    public AddElementsFromSocket shallowClone() {
        return new AddElementsFromSocket.Builder()
                .hostname(hostname)
                .port(port)
                .generator(elementGenerator)
                .parallelism(parallelism)
                .validate(validate)
                .skipInvalidElements(skipInvalidElements)
                .delimiter(delimiter)
                .options(options)
                .build();
    }

    public static class Builder extends Operation.BaseBuilder<AddElementsFromSocket, Builder>
            implements Validatable.Builder<AddElementsFromSocket, Builder> {
        public Builder() {
            super(new AddElementsFromSocket());
        }

        public Builder generator(final Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> generator) {
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

        public Builder delimiter(final String delimiter) {
            _getOp().setDelimiter(delimiter);
            return _self();
        }

        public Builder parallelism(final Integer parallelism) {
            _getOp().setParallelism(parallelism);
            return _self();
        }
    }
}
