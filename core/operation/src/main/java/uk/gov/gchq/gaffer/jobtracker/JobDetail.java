/*
 * Copyright 2016-2019 Crown Copyright
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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.operation.OperationChain;

import java.io.Serializable;

/**
 * POJO containing details of a Gaffer job.
 */
public class JobDetail implements Serializable {
    private static final long serialVersionUID = -1677432285205724269L;
    private static final String CHARSET_NAME = CommonConstants.UTF_8;
    private String parentJobId;
    private Repeat repeat;
    private String jobId;
    private String userId;
    private JobStatus status;
    private Long startTime;
    private Long endTime;
    private String opChain;
    private String description;

    public JobDetail() {
    }

    public JobDetail(final JobDetail oldJobDetail, final JobDetail newJobDetail) {
        this.jobId = getNewOrOld(oldJobDetail.jobId, newJobDetail.jobId);
        this.userId = getNewOrOld(oldJobDetail.userId, newJobDetail.userId);
        this.opChain = getNewOrOld(oldJobDetail.opChain, newJobDetail.opChain);
        this.description = getNewOrOld(oldJobDetail.description, newJobDetail.description);
        this.status = getNewOrOld(oldJobDetail.status, newJobDetail.status);
        this.parentJobId = getNewOrOld(oldJobDetail.parentJobId, newJobDetail.parentJobId);
        this.repeat = getNewOrOld(oldJobDetail.repeat, newJobDetail.repeat);

        if (null == oldJobDetail.startTime) {
            this.startTime = System.currentTimeMillis();
        } else {
            this.startTime = oldJobDetail.startTime;
            this.endTime = System.currentTimeMillis();
        }
    }

    public JobDetail(final String jobId, final String userId, final OperationChain<?> opChain, final JobStatus jobStatus, final String description) {
        this(jobId, null, userId, opChain, jobStatus, description);
    }

    public JobDetail(final String jobId, final String userId, final String opChain, final JobStatus jobStatus, final String description) {
        this(jobId, null, userId, opChain, jobStatus, description);
    }

    public JobDetail(final String jobId, final String parentJobId, final String userId, final OperationChain<?> opChain, final JobStatus jobStatus, final String description) {
        this.jobId = jobId;
        this.parentJobId = parentJobId;
        this.userId = userId;
        this.startTime = System.currentTimeMillis();
        this.status = jobStatus;
        this.opChain = null != opChain ? opChain.toOverviewString() : "";
        this.description = description;
    }

    public JobDetail(final String jobId, final String parentJobId, final String userId, final String opChain, final JobStatus jobStatus, final String description) {
        setOpChain(opChain);
        this.jobId = jobId;
        this.userId = userId;
        this.startTime = System.currentTimeMillis();
        this.status = jobStatus;
        this.description = description;
        this.parentJobId = parentJobId;
    }

    public JobDetail(final String jobId, final String parentJobId, final String userId, final String opChain, final JobStatus jobStatus, final String description, final Repeat repeat) {
        this(jobId, parentJobId, userId, opChain, jobStatus, description);
        this.repeat = repeat;
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

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(final Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(final Long endTime) {
        this.endTime = endTime;
    }

    public void setParentJobId(final String parentJobId) {
        this.parentJobId = parentJobId;
    }

    public String getParentJobId() {
        return parentJobId;
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

    public Repeat getRepeat() {
        return repeat;
    }

    public void setRepeat(final Repeat repeat) {
        this.repeat = repeat;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }
        final JobDetail jobDetail = (JobDetail) obj;
        return new EqualsBuilder()
                .append(jobId, jobDetail.jobId)
                .append(userId, jobDetail.userId)
                .append(opChain, jobDetail.opChain)
                .append(startTime, jobDetail.startTime)
                .append(endTime, jobDetail.endTime)
                .append(status, jobDetail.status)
                .append(description, jobDetail.description)
                .append(parentJobId, jobDetail.parentJobId)
                .append(repeat, jobDetail.repeat)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(23, 53)
                .append(jobId)
                .append(userId)
                .append(opChain)
                .append(startTime)
                .append(endTime)
                .append(status)
                .append(description)
                .append(parentJobId)
                .append(repeat)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("jobId", jobId)
                .append("userId", userId)
                .append("status", status)
                .append("startTime", startTime)
                .append("endTime", endTime)
                .append("opChain", opChain)
                .append("description", description)
                .append("parentJobId", parentJobId)
                .append("repeat", repeat)
                .toString();
    }

    private <T> T getNewOrOld(final T oldValue, final T newValue) {
        return null == newValue ? oldValue : newValue;
    }

    public static final class Builder {
        private String parentJobId;
        private Repeat repeat;
        private String jobId;
        private String userId;
        private JobStatus status;
        private String opChain;
        private String description;

        public Builder parentJobId(final String parentJobId) {
            this.parentJobId = parentJobId;
            return this;
        }

        public Builder repeat(final Repeat repeat) {
            this.repeat = repeat;
            return this;
        }

        public Builder jobId(final String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder userId(final String userId) {
            this.userId = userId;
            return this;
        }

        public Builder status(final JobStatus status) {
            this.status = status;
            return this;
        }

        public Builder opChain(final String opChain) {
            this.opChain = opChain;
            return this;
        }

        public Builder opChain(final OperationChain opChain) {
            this.opChain = opChain != null ? opChain.toOverviewString() : "";
            return this;
        }

        public Builder description(final String description) {
            this.description = description;
            return this;
        }

        public JobDetail build() {
            return new JobDetail(jobId, parentJobId, userId, opChain, status, description, repeat);
        }
    }
}
