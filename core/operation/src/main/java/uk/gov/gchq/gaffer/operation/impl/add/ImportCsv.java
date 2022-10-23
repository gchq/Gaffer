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

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Validatable;
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
public class ImportCsv implements
        Operation,
        Validatable {

    @Required
    private String filename;
    private char delimiter = ',';
    private String nullString = "";
    private boolean trim = false;
    private boolean validate = true;
    private boolean skipInvalidElements;
    private Map<String, String> options;
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
    public String getFilename() {
        return filename;
    }

    public void setFilename(final String filename) {
        this.filename = filename;
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
    public ImportCsv shallowClone() {
        return new Builder()
                .filename(filename)
                .validate(validate)
                .skipInvalidElements(skipInvalidElements)
                .options(options)
                .build();
    }

    public static class Builder extends BaseBuilder<ImportCsv, Builder>
            implements Validatable.Builder<ImportCsv, Builder> {
        public Builder() {
            super(new ImportCsv());
        }

        public Builder filename(final String filename) {
            _getOp().setFilename(filename);
            return _self();
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
    }
}
