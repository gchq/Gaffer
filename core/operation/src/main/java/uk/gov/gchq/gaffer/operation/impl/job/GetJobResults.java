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

package uk.gov.gchq.gaffer.operation.impl.job;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.export.Export;
import uk.gov.gchq.gaffer.operation.export.GetExport;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.GetGafferResultCacheExport;
import uk.gov.gchq.koryphe.Since;

import java.util.Map;

/**
 * A {@code GetJobResults} operation is used to retrieve the results of executing
 * a job on a Gaffer graph.
 */
@JsonPropertyOrder(value = {"class"}, alphabetic = true)
@Since("1.0.0")
public class GetJobResults extends GetGafferResultCacheExport {
    private Map<String, String> options;

    @JsonIgnore
    @Override
    public String getKey() {
        return null;
    }

    @Override
    public void setKey(final String key) {
        if (null != key && !Export.DEFAULT_KEY.equals(key)) {
            throw new IllegalArgumentException("Keys cannot be used with this operation");
        }
    }

    @Override
    public GetJobResults shallowClone() {
        return new GetJobResults.Builder()
                .jobId(getJobId())
                .options(options)
                .build();
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    public static class Builder
            extends Operation.BaseBuilder<GetJobResults, Builder>
            implements GetExport.Builder<GetJobResults, Builder> {
        public Builder() {
            super(new GetJobResults());
        }
    }
}
