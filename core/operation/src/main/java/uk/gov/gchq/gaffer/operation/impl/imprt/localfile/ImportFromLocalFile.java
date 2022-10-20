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

package uk.gov.gchq.gaffer.operation.impl.imprt.localfile;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;

import uk.gov.gchq.gaffer.data.generator.CsvElementGenerator;
import uk.gov.gchq.gaffer.operation.imprt.ImportFrom;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * A {@code ImportFromLocalFile} operation that takes a local file
 * and reads its contents to Iterable of Strings.
 */
@JsonPropertyOrder(value = {"class", "input", "filePath"}, alphabetic = true)
@Since("2.0.0")
@Summary("Imports CSV data from a local file")
public class ImportFromLocalFile implements InputOutput<String,Iterable<? extends String>> {

    @Required
    private String filePath;

    private Map<String, String> options;

    @Override
    public ImportFromLocalFile shallowClone() throws CloneFailedException {
        return new ImportFromLocalFile.Builder()
                .input(filePath)
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
    public final String getInput() {
        return filePath;
    }

    @Override
    public void setInput(final String filePath) {
        this.filePath = filePath;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public TypeReference<String> getOutputTypeReference() {
        return (TypeReference) new TypeReferenceImpl.Object();
    }



    public static final class Builder extends BaseBuilder<ImportFromLocalFile, Builder>
            implements ImportFrom.Builder<ImportFromLocalFile, String, Builder> {

        public Builder() {
            super(new ImportFromLocalFile());
        }
    }

    public CsvElementGenerator.Builder delimiter(final char delimiter) {
        this.delimiter = delimiter;
        return this;
    }
}


