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

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.ExportToGafferResultCache;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEdges;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobResults;
import uk.gov.gchq.gaffer.user.User;

public class GetJobResultsExample extends OperationExample {
    private String jobId;

    public static void main(final String[] args) throws OperationException {
        new GetJobResultsExample().run();
    }

    public GetJobResultsExample() {
        super(GetJobResults.class);
    }

    @Override
    public void runExamples() {
        try {
            final OperationChain<JobDetail> opChain = new OperationChain.Builder()
                    .first(new GetAllEdges())
                    .then(new ExportToGafferResultCache())
                    .then(new GetJobDetails())
                    .build();
            final JobDetail jobDetails = getGraph().execute(opChain, new User("user01"));
            jobId = jobDetails.getJobId();
        } catch (final OperationException e) {
            throw new RuntimeException(e);
        }

        getJobResults();
    }

    public CloseableIterable<?> getJobResults() {
        // ---------------------------------------------------------
        final GetJobResults getJobResults = new GetJobResults.Builder()
                .jobId(jobId)
                .build();
        // ---------------------------------------------------------

        return runExample(getJobResults);
    }
}
