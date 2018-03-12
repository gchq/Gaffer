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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;

import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;

import java.util.Map;

/**
 * A {@code GetJobDetails} operation is used to retrieve the details of a single
 * job from a Gaffer graph.
 */
@JsonPropertyOrder(value = {"class"}, alphabetic = true)
@Since("1.0.0")
public class GetJobDetails implements
        Output<JobDetail> {
    private String jobId;
    private Map<String, String> options;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(final String jobId) {
        this.jobId = jobId;
    }

    @Override
    public TypeReference<JobDetail> getOutputTypeReference() {
        return new TypeReferenceImpl.JobDetail();
    }

    @Override
    public GetJobDetails shallowClone() {
        return new GetJobDetails.Builder()
                .jobId(jobId)
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

    public static class Builder extends Operation.BaseBuilder<GetJobDetails, Builder>
            implements Output.Builder<GetJobDetails, JobDetail, Builder> {
        public Builder() {
            super(new GetJobDetails());
        }

        public Builder jobId(final String jobId) {
            _getOp().setJobId(jobId);
            return this;
        }
    }
}
