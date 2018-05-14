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

package uk.gov.gchq.gaffer.operation.impl.export.set;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.export.GetExport;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * An {@code GetSetExport} GetExport operation gets exported Set results.
 * The Set export is maintained per single Job or {@link uk.gov.gchq.gaffer.operation.OperationChain} only.
 * It cannot be used across multiple separate operation requests.
 * So ExportToSet and GetSetExport must be used inside a single operation chain.
 */
@JsonPropertyOrder(value = {"class", "start", "end"}, alphabetic = true)
@Since("1.0.0")
@Summary("Fetches data from a Set cache")
public class GetSetExport implements
        GetExport,
        Output<Iterable<?>> {
    private String jobId;
    private String key;
    private int start = 0;
    private Integer end = null;
    private Map<String, String> options;

    public int getStart() {
        return start;
    }

    public void setStart(final int start) {
        this.start = start;
    }

    public Integer getEnd() {
        return end;
    }

    public void setEnd(final Integer end) {
        this.end = end;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public void setKey(final String key) {
        this.key = key;
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public void setJobId(final String jobId) {
        this.jobId = jobId;
    }

    @Override
    public TypeReference<Iterable<?>> getOutputTypeReference() {
        return new TypeReferenceImpl.IterableObj();
    }

    @Override
    public GetSetExport shallowClone() {
        return new GetSetExport.Builder()
                .jobId(jobId)
                .key(key)
                .start(start)
                .end(end)
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

    public static class Builder
            extends Operation.BaseBuilder<GetSetExport, Builder>
            implements GetExport.Builder<GetSetExport, Builder>,
            Output.Builder<GetSetExport, Iterable<?>, Builder> {
        public Builder() {
            super(new GetSetExport());
        }

        public Builder start(final int start) {
            _getOp().setStart(start);
            return _self();
        }

        public Builder end(final Integer end) {
            _getOp().setEnd(end);
            return _self();
        }
    }
}
