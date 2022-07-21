/*
 * Copyright 2022 Crown Copyright
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
import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.data.element.Element;
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
 * @see ToOpenCypherCsv.Builder
 */
@JsonPropertyOrder(value = {"class", "input", "elementGenerator"}, alphabetic = true)
@Since("2.0.0")
@Summary("Converts elements to CSV Strings")
public class ToOpenCypherCsv implements
        InputOutput<Iterable<? extends Element>, Iterable<? extends String>>,
        MultiInput<Element> {

    private Iterable<? extends Element> input;
    private boolean neo4jFormat = false;

    @Override
    public Iterable<? extends Element> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends Element> input) {
        this.input = input;
    }

    @Override
    public TypeReference<Iterable<? extends String>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableString();
    }

    @Override
    public ToOpenCypherCsv shallowClone() {
        return new ToOpenCypherCsv.Builder()
                .input(input)
                .neo4jFormat(neo4jFormat)
                .build();
    }

    @Override
    public Map<String, String> getOptions() {
        return null;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        // options aren't used by this class
    }

    public void setNeo4jFormat(final boolean neo4jFormat) {
        this.neo4jFormat = neo4jFormat;
    }
    public boolean isNeo4jFormat() {
        return neo4jFormat;
    }

    public static final class Builder extends BaseBuilder<ToOpenCypherCsv, Builder>
            implements InputOutput.Builder<ToOpenCypherCsv, Iterable<? extends Element>, Iterable<? extends String>, Builder>,
            MultiInput.Builder<ToOpenCypherCsv, Element, Builder> {
        public Builder() {
            super(new ToOpenCypherCsv());
        }

        public ToOpenCypherCsv.Builder neo4jFormat(final boolean neo4jFormat) {
            _getOp().setNeo4jFormat(neo4jFormat);
            return _self();
        }

    }
}
