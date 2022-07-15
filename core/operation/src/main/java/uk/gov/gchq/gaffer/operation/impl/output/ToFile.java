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

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.koryphe.Summary;

import java.io.File;
import java.util.Map;

@Summary("Outputs an Iterable of strings to a CSV file")
public class ToFile implements
        InputOutput<Iterable<? extends String>, File>, MultiInput<String> {

    private String filePath;
    private Iterable<? extends String> lines;
    private Map<String, String> options;

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(final String filePath) {
        this.filePath = filePath;
    }

    @Override
    public Operation shallowClone() throws CloneFailedException {
        return new ToFile.Builder()
                .filePath(filePath)
                .input(lines)
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

    @Override
    public Iterable<? extends String> getInput() {
        return lines;
    }

    @Override
    public void setInput(final Iterable<? extends String> input) {
        this.lines = input;
    }

    @Override
    public TypeReference<File> getOutputTypeReference() {
        return null;
    }

    public static final class Builder extends BaseBuilder<ToFile, ToFile.Builder>
            implements InputOutput.Builder<ToFile, Iterable<? extends String>, File, ToFile.Builder>,
            MultiInput.Builder<ToFile, String, ToFile.Builder> {
        public Builder() {
            super(new ToFile());
        }
        /**
         * @param filePath the location of the file to output the Iterable<? extends String> to
         * @return this Builder
         */
        public ToFile.Builder filePath(final String filePath) {
            _getOp().setFilePath(filePath);
            return _self();
        }
    }
}
