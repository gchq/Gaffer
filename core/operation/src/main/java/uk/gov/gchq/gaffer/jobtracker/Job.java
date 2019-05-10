/*
 * Copyright 2019 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;

/**
 * POJO containing details of a scheduled Gaffer job,
 * a {@link Repeat} and an Operation.
 * To be used within the ExecuteJob for a ScheduledJob.
 */
public class Job {
    private Repeat repeat;
    private Operation operation;

    public Job() {
    }

    public Job(final Repeat repeat) {
        this(repeat, null);
    }

    public Job(final Repeat repeat, final Operation operation) {
        this.repeat = repeat;
        this.operation = operation;
    }

    public Repeat getRepeat() {
        return repeat;
    }

    public void setRepeat(final Repeat repeat) {
        this.repeat = repeat;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setOperation(final Operation operation) {
        this.operation = operation;
    }


    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }
        final Job job = (Job) obj;

        try {
            return new EqualsBuilder()
                    .append(JSONSerialiser.serialise(operation), JSONSerialiser.serialise(job.operation))
                    .append(repeat, job.repeat)
                    .isEquals();
        } catch (final SerialisationException e) {
            throw new GafferRuntimeException("Unable to compare operations as one is not JSON serialisable", e);
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 53)
                .append(operation)
                .append(repeat)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("operation", operation)
                .append("repeat", repeat)
                .toString();
    }
}
