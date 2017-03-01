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
package uk.gov.gchq.gaffer.example.gettingstarted.analytic;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.example.gettingstarted.generator.DataGenerator15;
import uk.gov.gchq.gaffer.example.gettingstarted.util.DataUtils;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jobtracker.JobStatus;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import uk.gov.gchq.gaffer.operation.impl.job.GetAllJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobResults;
import uk.gov.gchq.gaffer.user.User;

public class LoadAndQuery15 extends LoadAndQuery {
    public LoadAndQuery15() {
        super("Jobs");
    }

    public static void main(final String[] args) throws OperationException {
        new LoadAndQuery15().run();
    }

    public CloseableIterable<?> run() throws OperationException {
        // [user] Create a user
        // ---------------------------------------------------------
        final User user = new User("user01");
        // ---------------------------------------------------------

        // [graph] create a graph using our schema and store properties
        // ---------------------------------------------------------
        final Graph graph = new Graph.Builder()
                .addSchemas(getSchemas())
                .storeProperties(getStoreProperties())
                .build();
        // ---------------------------------------------------------

        // [add] add the edges to the graph
        // ---------------------------------------------------------
        final OperationChain addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .generator(new DataGenerator15())
                        .objects(DataUtils.loadData(getData()))
                        .build())
                .then(new AddElements())
                .build();

        graph.execute(addOpChain, user);
        // ---------------------------------------------------------

        // [job] create an operation chain to be executed as a job
        // ---------------------------------------------------------
        final OperationChain<CloseableIterable<Edge>> job = new OperationChain.Builder()
                .first(new GetEdges.Builder<EntitySeed>()
                        .addSeed(new EntitySeed("1"))
                        .build())
                .build();
        // ---------------------------------------------------------

        // [execute job] execute the job
        // ---------------------------------------------------------
        final JobDetail initialJobDetail = graph.executeJob(job, user);
        final String jobId = initialJobDetail.getJobId();
        // ---------------------------------------------------------
        log("JOB_DETAIL_START", initialJobDetail.toString());

        waitUntilJobHashFinished(user, graph, initialJobDetail);

        // [job details] Get the job details
        // ---------------------------------------------------------
        final JobDetail jobDetail = graph.execute(
                new GetJobDetails.Builder()
                        .jobId(jobId)
                        .build(),
                user);
        // ---------------------------------------------------------
        log("JOB_DETAIL_FINISH", jobDetail.toString());


        // [all job details] Get all job details
        // ---------------------------------------------------------
        final CloseableIterable<JobDetail> jobDetails = graph.execute(new GetAllJobDetails(), user);
        // ---------------------------------------------------------
        for (final JobDetail detail : jobDetails) {
            log("ALL_JOB_DETAILS", detail.toString());
        }

        // [get job results] Get the job results
        // ---------------------------------------------------------
        final CloseableIterable<?> jobResults = graph.execute(new GetJobResults.Builder()
                .jobId(jobId)
                .build(), user);
        // ---------------------------------------------------------
        for (final Object result : jobResults) {
            log("JOB_RESULTS", result.toString());
        }

        return jobResults;
    }

    private void waitUntilJobHashFinished(final User user, final Graph graph, final JobDetail initialJobDetail) throws OperationException {
        JobDetail jobDetail = initialJobDetail;
        while (JobStatus.RUNNING.equals(jobDetail.getStatus())) {
            jobDetail = graph.execute(new GetJobDetails.Builder()
                    .jobId(jobDetail.getJobId())
                    .build(), user);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
