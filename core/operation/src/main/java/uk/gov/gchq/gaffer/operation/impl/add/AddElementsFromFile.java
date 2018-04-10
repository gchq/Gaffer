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
package uk.gov.gchq.gaffer.operation.impl.add;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Validatable;
import uk.gov.gchq.koryphe.Since;

import java.util.Map;
import java.util.function.Function;

/**
 * An {@code AddElementsFromFile} operation takes a filename, converts each
 * line of the file to a Gaffer {@link Element} using the provided
 * {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} then adds these
 * elements to the Graph.
 *
 * @see Builder
 */
@JsonPropertyOrder(value = {"class", "filename", "elementGenerator"}, alphabetic = true)
@Since("1.0.0")
public class AddElementsFromFile implements
        Operation,
        Validatable {
    /**
     * The fully qualified path of the file from which Flink should consume
     */
    @Required
    private String filename;

    @Required
    private Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> elementGenerator;

    /**
     * The parallelism of the job to be created
     */
    private Integer parallelism;
    private boolean validate = true;
    private boolean skipInvalidElements;
    private Map<String, String> options;

    public String getFilename() {
        return filename;
    }

    public void setFilename(final String filename) {
        this.filename = filename;
    }

    public void setParallelism(final Integer parallelism) {
        this.parallelism = parallelism;
    }

    public Integer getParallelism() {
        return this.parallelism;
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

    @Override
    public AddElementsFromFile shallowClone() {
        return new AddElementsFromFile.Builder()
                .filename(filename)
                .generator(elementGenerator)
                .parallelism(parallelism)
                .validate(validate)
                .skipInvalidElements(skipInvalidElements)
                .options(options)
                .build();
    }

    public static class Builder extends BaseBuilder<AddElementsFromFile, Builder>
            implements Validatable.Builder<AddElementsFromFile, Builder> {
        public Builder() {
            super(new AddElementsFromFile());
        }

        public Builder generator(final Class<? extends Function<Iterable<? extends String>, Iterable<? extends Element>>> generator) {
            _getOp().setElementGenerator(generator);
            return _self();
        }

        public Builder filename(final String filename) {
            _getOp().setFilename(filename);
            return _self();
        }

        public Builder parallelism(final Integer parallelism) {
            _getOp().setParallelism(parallelism);
            return _self();
        }
    }
}
