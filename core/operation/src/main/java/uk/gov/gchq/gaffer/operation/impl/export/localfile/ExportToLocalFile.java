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

package uk.gov.gchq.gaffer.operation.impl.export.localfile;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;

import uk.gov.gchq.gaffer.operation.export.ExportTo;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl.IterableString;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * A {@code ExportToLocalFile} operation that takes an Iterable of Strings
 * and writes them to a local file.
 */
@JsonPropertyOrder(value = {"class", "input", "filePath"}, alphabetic = true)
@Since("2.0.0")
@Summary("Exports elements to a local file")
public class ExportToLocalFile implements ExportTo<Iterable<? extends String>> {

    @Required
    private String filePath;

    private Iterable<? extends String> input;
    private Map<String, String> options;

    public final String getFilePath() {
        return filePath;
    }

    public void setFilePath(final String filePath) {
        this.filePath = filePath;
        setKey(filePath);
    }

    @Override
    public ExportToLocalFile shallowClone() throws CloneFailedException {
        return new ExportToLocalFile.Builder()
                .filePath(filePath)
                .input(input)
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

    @Override
    public final String getKey() {
        return filePath;
    }

    @Override
    public void setKey(final String key) {
        // key is not used
    }


    @Override
    public final Iterable<? extends String> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends String> input) {
        this.input = input;
    }

    @Override
    public TypeReference<Iterable<? extends String>> getOutputTypeReference() {
        return new IterableString();
    }


    public static final class Builder extends BaseBuilder<ExportToLocalFile, Builder>
            implements ExportTo.Builder<ExportToLocalFile, Iterable<? extends String>, Builder> {

        public Builder() {
            super(new ExportToLocalFile());
        }

        public Builder filePath(final String filePath) {
            _getOp().setFilePath(filePath);
            return _self();
        }
    }
}
