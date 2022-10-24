/*
 * Copyright 2016-2020 Crown Copyright
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

import org.codehaus.jackson.annotate.JsonIgnore;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.export.GetExport;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * An {@code ImportFromLocalFile} GetExport operation gets exported Set results.
 * The Set export is maintained per single Job or {@link uk.gov.gchq.gaffer.operation.OperationChain} only.
 * It cannot be used across multiple separate operation requests.
 * So ExportToSet and ImportFromLocalFile must be used inside a single operation chain.
 */
@JsonPropertyOrder(value = {"class", "start", "end"}, alphabetic = true)
@Since("1.0.0")
@Summary("Fetches data from a local file")
public class ImportFromLocalFile implements
        GetExport,
        Output<Iterable<String>> {

    @Required
    private String filePath;

    @JsonIgnore
    private String key;

    private Map<String, String> options;

    public String getFilePath() {
        return getKey();
    }

    public void setFilePath(final String filePath) {
        setKey(filePath);
    }

    @Override
    @JsonIgnore
    public String getKey() {
        return key;
    }

    @Override
    @JsonIgnore
    public void setKey(final String key) {
        this.key = key;
    }


    @Override
    public TypeReference<Iterable<String>> getOutputTypeReference() {
        return (TypeReference) new TypeReferenceImpl.IterableString();
    }

    @Override
    public ImportFromLocalFile shallowClone() {
        return new ImportFromLocalFile.Builder()
                .key(key)
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
    public String getJobId() {
        return null;
    }

    @Override
    public void setJobId(final String jobId) {

    }

    public static class Builder
            extends Operation.BaseBuilder<ImportFromLocalFile, ImportFromLocalFile.Builder>
            implements GetExport.Builder<ImportFromLocalFile, ImportFromLocalFile.Builder>,
            Output.Builder<ImportFromLocalFile, Iterable<String>, ImportFromLocalFile.Builder> {
        public Builder() {
            super(new ImportFromLocalFile());
        }

        public ImportFromLocalFile.Builder filePath(final String filePath) {
            _getOp().setFilePath(filePath);
            return _self();
        }

    }
}
