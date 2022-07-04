/*
 * Copyright 2017-2020 Crown Copyright
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
package uk.gov.gchq.gaffer.operation.impl.output;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.CsvGenerator;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * A {@code ToMap} operation takes in an {@link Iterable} of items
 * and uses a {@link uk.gov.gchq.gaffer.data.generator.CsvGenerator} to convert
 * each item into a CSV String.
 *
 * @see ToCsv.Builder
 */
@JsonPropertyOrder(value = {"class", "input", "elementGenerator"}, alphabetic = true)
@Since("1.0.0")
@Summary("Converts elements to CSV Strings")
public class ToCsv implements
        InputOutput<Iterable<? extends Element>, Iterable<? extends String>>,
        MultiInput<Element> {

    @Required
    private CsvGenerator elementGenerator;
    private Iterable<? extends Element> input;
    private boolean includeHeader = true;
    private boolean openCypherFormat = false;
    private boolean neo4jFormat = false;
    private Map<String, String> options;
    private boolean quoted = false;

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public CsvGenerator getElementGenerator() {
            return elementGenerator;
    }
    private void setElementGenerator(final CsvGenerator elementGenerator) {
        this.elementGenerator = elementGenerator;
    }
    private void setOpenCypherFormat(final boolean openCypherFormat) {
        this.openCypherFormat = openCypherFormat;
    }
    public boolean isOpenCypherFormat() {
        return openCypherFormat;
    }
    @Override
    public Iterable<? extends Element> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends Element> input) {
        this.input = input;
    }

    public boolean isIncludeHeader() {
        return includeHeader;
    }

    private void setIncludeHeader(final boolean includeHeader) {
        this.includeHeader = includeHeader;
    }

    @Override
    public TypeReference<Iterable<? extends String>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableString();
    }

    @Override
    public ToCsv shallowClone() {
        return new ToCsv.Builder()
                .generator(elementGenerator)
                .input(input)
                .openCypherFormat(openCypherFormat)
                .neo4jFormat(neo4jFormat)
                .includeHeader(includeHeader)
                .options(options)
                .build();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public boolean isQuoted() {
        return quoted;
    }

    public void setQuoted(final boolean quoted) {
        this.quoted = quoted;
    }
    public void setNeo4jFormat(final boolean neo4jFormat) {
        this.neo4jFormat = neo4jFormat;
    }
    public boolean isNeo4jFormat() {
        return neo4jFormat;
    }


    public static final class Builder extends BaseBuilder<ToCsv, Builder>
            implements InputOutput.Builder<ToCsv, Iterable<? extends Element>, Iterable<? extends String>, Builder>,
            MultiInput.Builder<ToCsv, Element, Builder> {
        public Builder() {
            super(new ToCsv());
        }

        /**
         * @param generator the {@link uk.gov.gchq.gaffer.data.generator.ElementGenerator} to set on the operation
         * @return this Builder
         */
        public ToCsv.Builder generator(final CsvGenerator generator) {
            _getOp().setElementGenerator(generator);
            return _self();
        }

        public ToCsv.Builder includeHeader(final boolean includeHeader) {
            _getOp().setIncludeHeader(includeHeader);
            return _self();
        }
        public ToCsv.Builder openCypherFormat(final boolean openCypherFormat) {
            _getOp().setOpenCypherFormat(openCypherFormat);
            return _self();
        }
        public ToCsv.Builder neo4jFormat(final boolean neo4jFormat) {
            _getOp().setNeo4jFormat(neo4jFormat);
            return _self();
        }

        public ToCsv.Builder quoted(final boolean quoted) {
            _getOp().setQuoted(quoted);
            return _self();
        }
    }
}
