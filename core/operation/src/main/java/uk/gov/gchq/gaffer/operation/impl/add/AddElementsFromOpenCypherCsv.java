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
 * An {@code AddElementsFromOpenCypherCsv} operation takes a filename, converts each
 * line of the file to a Gaffer Element then adds these
 * elements to the Graph. The file must be of valid openCypher CSV format.
 *
 * @see Builder
 */

@JsonPropertyOrder(value = {"class", "filename"}, alphabetic = true)
@Since("2.0.0")
@Summary("Adds elements from a openCypher CSV file")
public class AddElementsFromOpenCypherCsv implements
        Operation,
        Validatable {

    @Required
    private String filename;

    private boolean validate = true;
    private boolean skipInvalidElements;
    private Map<String, String> options;

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
    public AddElementsFromOpenCypherCsv shallowClone() {
        return new Builder()
                .filename(filename)
                .validate(validate)
                .skipInvalidElements(skipInvalidElements)
                .options(options)
                .build();
    }

    public static class Builder extends BaseBuilder<AddElementsFromOpenCypherCsv, Builder>
            implements Validatable.Builder<AddElementsFromOpenCypherCsv, Builder> {
        public Builder() {
            super(new AddElementsFromOpenCypherCsv());
        }

        public Builder filename(final String filename) {
            _getOp().setFilename(filename);
            return _self();
        }
        // add delimiter, ......
    }
}
