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

package uk.gov.gchq.gaffer.operation.impl.add;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.CsvFormat;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Validatable;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * An {@code ImportCsv} operation takes a filename, converts each
 * line of the file to an element then adds these
 * elements to the graph. The file must be in the openCypher CSV format.
 *
 * @see Builder
 * @see <a href="https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-opencypher.html">openCypher</a>
 */

@JsonPropertyOrder(value = {"class", "filename"}, alphabetic = true)
@Since("2.0.0")
@Summary("Adds elements from a openCypher CSV file")
public class CsvToElements implements
        Operation,
        Validatable,
        MultiInput<String>,
        InputOutput<Iterable<? extends String>, Iterable<? extends Element>> {
    private char delimiter = ',';
    private String nullString = "";
    private boolean trim = false;
    private boolean validate = true;
    private boolean skipInvalidElements;
    private Map<String, String> options;
    private Iterable<? extends String> input;
    private CsvFormat csvFormat;

    public char getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(final char delimiter) {
        this.delimiter = delimiter;
    }

    public String getNullString() {
        return nullString;
    }

    public void setNullString(final String nullString) {
        this.nullString = nullString;
    }

    public boolean isTrim() {
        return trim;
    }

    public void setTrim(final boolean trim) {
        this.trim = trim;
    }

    public CsvFormat getCsvFormat() {
        return csvFormat;
    }

    public void setCsvFormat(final CsvFormat csvFormat) {
        this.csvFormat = csvFormat;
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
    public CsvToElements shallowClone() {
        return new Builder()
                .validate(validate)
                .skipInvalidElements(skipInvalidElements)
                .options(options)
                .input(input)
                .trim(trim)
                .delimiter(delimiter)
                .nullString(nullString)
                .csvFormat(csvFormat)
                .build();
    }

    @Override
    public Iterable<? extends String> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends String> csvLines) {
        this.input = csvLines;
    }

    @Override
    public TypeReference<Iterable<? extends Element>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableElement();
    }

    public static class Builder extends BaseBuilder<CsvToElements, Builder>
            implements Validatable.Builder<CsvToElements, Builder>,
            InputOutput.Builder<CsvToElements, Iterable<? extends String>, Iterable<? extends Element>, CsvToElements.Builder>,
            MultiInput.Builder<CsvToElements, String, CsvToElements.Builder> {
        public Builder() {
            super(new CsvToElements());
        }

        public Builder delimiter(final char delimiter) {
            _getOp().setDelimiter(delimiter);
            return _self();
        }

        public Builder trim(final Boolean trim) {
            _getOp().setTrim(trim);
            return _self();
        }

        public Builder nullString(final String nullString) {
            _getOp().setNullString(nullString);
            return _self();
        }

        public Builder csvFormat(final CsvFormat csvFormat) {
            _getOp().setCsvFormat(csvFormat);
            return _self();
        }
    }
}
