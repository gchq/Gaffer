/*
 * Copyright 2016-2017 Crown Copyright
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

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetExport;

public class GetGafferResultCacheExport implements
        Operation,
        GetExport {
    private String jobId;
    private String key;

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

    public static class Builder
            extends Operation.BaseBuilder<GetGafferResultCacheExport, Builder>
            implements GetExport.Builder<GetGafferResultCacheExport, Builder> {
        public Builder() {
            super(new GetGafferResultCacheExport());
        }
    }
}
