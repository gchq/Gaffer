/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.jobtracker;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.operation.OperationChain;
import java.io.Serializable;

public class JobDetail implements Serializable {
    private static final long serialVersionUID = -1677432285205724269L;
    private String jobId;
    private String userId;
    private JobStatus status;
    private Long timestamp;
    private String opChain;
    private String description;

    public JobDetail() {
    }

    public JobDetail(final String jobId, final String userId, final OperationChain<?> opChain, final JobStatus jobStatus, final String description) {
        final String opChainString = null != opChain ? opChain.toString() : "";
        this.jobId = jobId;
        this.userId = userId;
        this.timestamp = System.currentTimeMillis();
        this.status = jobStatus;
        this.opChain = opChainString;
        this.description = description;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(final String jobId) {
        this.jobId = jobId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(final String userId) {
        this.userId = userId;
    }

    public JobStatus getStatus() {
        return status;
    }

    public void setStatus(final JobStatus status) {
        this.status = status;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getOpChain() {
        return opChain;
    }

    public void setOpChain(final String opChain) {
        this.opChain = opChain;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    @Override
    public boolean equals(final Object obj) {
        return null != obj
                && (obj instanceof JobDetail)
                && equals((JobDetail) obj);
    }

    public boolean equals(final JobDetail jobDetail) {
        return null != jobDetail
                && new EqualsBuilder()
                .append(jobId, jobDetail.jobId)
                .append(userId, jobDetail.userId)
                .append(opChain, jobDetail.opChain)
                .append(timestamp, jobDetail.timestamp)
                .append(status, jobDetail.status)
                .append(description, jobDetail.description)
                .isEquals();
    }

    public int hashCode() {
        return new HashCodeBuilder(23, 5)
                .append(jobId)
                .append(userId)
                .append(opChain)
                .append(timestamp)
                .append(status)
                .append(description)
                .toHashCode();
    }

    public String toString() {
        return new ToStringBuilder(this)
                .append("jobId", jobId)
                .append("userId", userId)
                .append("status", status)
                .append("timestamp", timestamp)
                .append("opChain", opChain)
                .append("description", description)
                .toString();
    }
}
