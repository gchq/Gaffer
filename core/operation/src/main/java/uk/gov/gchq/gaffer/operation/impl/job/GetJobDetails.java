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

package uk.gov.gchq.gaffer.operation.impl.job;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.operation.AbstractOperation;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

public class GetJobDetails extends AbstractOperation<Object, JobDetail> {
    @JsonIgnore
    @Override
    public String getInput() {
        return (String) super.getInput();
    }

    @JsonIgnore
    @Override
    public void setInput(final Object input) {
        // Ignore the input if it isn't a string to allow chaining
        if (input instanceof String) {
            super.setInput(input);
        }
    }

    public String getJobId() {
        return (String) super.getInput();
    }

    public void setJobId(final String jobId) {
        super.setInput(jobId);
    }

    @Override
    protected TypeReference createOutputTypeReference() {
        return new TypeReferenceImpl.JobDetail();
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>> extends AbstractOperation.BaseBuilder<GetJobDetails, Object, JobDetail, CHILD_CLASS> {

        public BaseBuilder() {
            super(new GetJobDetails());
        }

        /**
         * @param jobId the jobId
         * @return this Builder
         * @see uk.gov.gchq.gaffer.operation.Operation#setInput(Object)
         */
        public CHILD_CLASS jobId(final String jobId) {
            return super.input(jobId);
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}
