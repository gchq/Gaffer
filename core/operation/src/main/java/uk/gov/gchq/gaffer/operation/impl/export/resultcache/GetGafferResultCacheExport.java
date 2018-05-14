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

package uk.gov.gchq.gaffer.operation.impl.export.resultcache;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.export.Export;
import uk.gov.gchq.gaffer.operation.export.GetExport;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * A {@code GetGafferResultCacheExport} operation is used to retrieve data which
 * has previously been exported to a Gaffer results cache.
 *
 * @see ExportToGafferResultCache
 */
@JsonPropertyOrder(value = {"class"}, alphabetic = true)
@Since("1.0.0")
@Summary("Fetches data from a Gaffer result cache")
public class GetGafferResultCacheExport implements
        GetExport,
        Output<CloseableIterable<?>> {
    private String jobId;
    private String key = Export.DEFAULT_KEY;
    private Map<String, String> options;

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
    public TypeReference<CloseableIterable<?>> getOutputTypeReference() {
        return new TypeReferenceImpl.CloseableIterableObj();
    }

    @Override
    public GetGafferResultCacheExport shallowClone() {
        return new GetGafferResultCacheExport.Builder()
                .jobId(jobId)
                .key(key)
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
            extends Operation.BaseBuilder<GetGafferResultCacheExport, Builder>
            implements GetExport.Builder<GetGafferResultCacheExport, Builder>,
            Output.Builder<GetGafferResultCacheExport, CloseableIterable<?>, Builder> {
        public Builder() {
            super(new GetGafferResultCacheExport());
        }
    }
}
