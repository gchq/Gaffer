/*
 * Copyright 2019-2020 Crown Copyright
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
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * A {@code CancelScheduledJob} is an {@link Operation} that will use the provided {@code jobId} to
 * cancel the job, if it is scheduled.
 */
@JsonPropertyOrder(value = {"jobId"}, alphabetic = true)
@Since("1.9.0")
@Summary("Cancels a scheduled job")
public class CancelScheduledJob implements Operation {
    @Required
    private String jobId;
    private Map<String, String> options;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(final String jobId) {
        this.jobId = jobId;
    }

    @Override
    public CancelScheduledJob shallowClone() throws CloneFailedException {
        return new CancelScheduledJob.Builder()
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

    public static class Builder extends Operation.BaseBuilder<CancelScheduledJob, CancelScheduledJob.Builder> {
        public Builder() {
            super(new CancelScheduledJob());
        }

        public Builder jobId(final String jobId) {
            _getOp().setJobId(jobId);
            return _self();
        }
    }
}
