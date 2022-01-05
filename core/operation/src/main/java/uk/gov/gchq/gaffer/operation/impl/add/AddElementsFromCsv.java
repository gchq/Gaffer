/*
 * Copyright 2016-2021 Crown Copyright
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
 * AddElementsFromCsv is an operation that generates elements from a csv file
 */

@JsonPropertyOrder(value = {"class", "filename", "elementGeneratorFilePath"}, alphabetic = true)
@Since("2.0.0")
@Summary("Adds elements from a csv file. Configure with a csv-to-element mappings file")
public class AddElementsFromCsv implements
        Operation,
        Validatable {

    @Required
    private String filename;

    private String elementGeneratorFilePath = "none";
    private String elementGeneratorClassName = "none";
    private String elementGeneratorJson = "none";


    private String delimiter = ",";
    private boolean quoted = false;
    private String quoteChar = "\"";

    private boolean validate = true;
    private boolean skipInvalidElements;
    private Map<String, String> options;

    public AddElementsFromCsv() {

    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(final String filename) {
        this.filename = filename;
    }

    public String getElementGeneratorFilePath() {
        return elementGeneratorFilePath;
    }

    public void setElementGeneratorFilePath(final String elementGeneratorFilePath) {
        this.elementGeneratorFilePath = elementGeneratorFilePath;
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

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(final String delimiter) {
        this.delimiter = delimiter;
    }

    public boolean isQuoted() {
        return quoted;
    }

    public void setQuoted(final boolean quoted) {
        this.quoted = quoted;
    }

    public String getQuoteChar() {
        return quoteChar;
    }

    public void setQuoteChar(final String quoteChar) {
        this.quoteChar = quoteChar;
    }

    @Override
    public boolean isValidate() {
        return validate;
    }

    @Override
    public void setValidate(final boolean validate) {
        this.validate = validate;
    }

    public String getElementGeneratorClassName() {
        return elementGeneratorClassName;
    }

    public void setElementGeneratorClassName(String elementGeneratorClassName) {
        this.elementGeneratorClassName = elementGeneratorClassName;
    }

    public String getElementGeneratorJson() {
        return elementGeneratorJson;
    }

    public void setElementGeneratorJson(String elementGeneratorJson) {
        this.elementGeneratorJson = elementGeneratorJson;
    }

    @Override
    public AddElementsFromCsv shallowClone() {
        return new Builder()
                .filename(filename)
                .elementGeneratorFilePath(elementGeneratorFilePath)
                .elementGeneratorClassName(elementGeneratorClassName)
                .elementGeneratorJson(elementGeneratorJson)
                .validate(validate)
                .skipInvalidElements(skipInvalidElements)
                .options(options)
                .delimiter(delimiter)
                .quoted(quoted)
                .quoteChar(quoteChar)
                .build();
    }

    public static class Builder extends BaseBuilder<AddElementsFromCsv, Builder>
            implements Validatable.Builder<AddElementsFromCsv, Builder> {
        public Builder() {
            super(new AddElementsFromCsv());
        }

        public Builder filename(final String filename) {
            _getOp().setFilename(filename);
            return _self();
        }

        public Builder elementGeneratorFilePath(final String elementGeneratorFilePath) {
            _getOp().setElementGeneratorFilePath(elementGeneratorFilePath);
            return _self();
        }

        public Builder elementGeneratorClassName(final String elementGeneratorClassName) {
            _getOp().setElementGeneratorClassName(elementGeneratorClassName);
            return _self();
        }

        public Builder elementGeneratorJson(final String elementGeneratorJson) {
            _getOp().setElementGeneratorJson(elementGeneratorJson);
            return _self();
        }

        public Builder delimiter(final String delimiter) {
            _getOp().setDelimiter(delimiter);
            return _self();
        }

        public Builder quoted(final boolean quoted) {
            _getOp().setQuoted(quoted);
            return _self();
        }

        public Builder quoteChar(final String quoteChar) {
            _getOp().setQuoteChar(quoteChar);
            return _self();
        }
    }
}
