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
package uk.gov.gchq.gaffer.example.operation;

import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEdges;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;

public class GetJobDetailsExample extends OperationExample {
    private String jobId;

    public static void main(final String[] args) throws OperationException {
        new GetJobDetailsExample().run();
    }

    public GetJobDetailsExample() {
        super(GetJobDetails.class);
    }

    @Override
    public void runExamples() {
        getJobDetailsInOperationChain();
        getJobDetails();
    }

    public JobDetail getJobDetails() {
        // ---------------------------------------------------------
        final GetJobDetails getJobDetails = new GetJobDetails.Builder()
                .jobId(jobId)
                .build();
        // ---------------------------------------------------------

        return runExample(getJobDetails);
    }

    public JobDetail getJobDetailsInOperationChain() {
        // ---------------------------------------------------------
        final OperationChain<JobDetail> opChain = new OperationChain.Builder()
                .first(new GetAllEdges())
                .then(new GetJobDetails())
                .build();
        // ---------------------------------------------------------

        final JobDetail jobDetail = runExample(opChain);
        jobId = jobDetail.getJobId();
        return jobDetail;
    }
}
