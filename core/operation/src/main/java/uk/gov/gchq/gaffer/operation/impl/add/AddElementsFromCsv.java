/*
 * Copyright 2016-2018 Crown Copyright
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

/*

 */

@JsonPropertyOrder(value = {"class", "filename", "mappingsFile"}, alphabetic = true)
@Since("1.6.0")
@Summary("Adds elements from a csv file. Configure with a csv-to-element mappings file")
public class AddElementsFromCsv implements
        Operation,
        Validatable {

    @Required
    private String filename;

    @Required
    private String mappingsFile;

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

    public String getMappingsFile() {
        return mappingsFile;
    }

    public void setMappingsFile(final String mappingsFile) {
        this.mappingsFile = mappingsFile;
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
    public AddElementsFromCsv shallowClone() {
        return new Builder()
                .filename(filename)
                .mappingsFile(mappingsFile)
                .validate(validate)
                .skipInvalidElements(skipInvalidElements)
                .options(options)
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

        public Builder mappingsFile(final String mappingsFile) {
            _getOp().setMappingsFile(mappingsFile);
            return _self();
        }
    }
}
