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
package uk.gov.gchq.gaffer.doc.dev.walkthrough;

import org.apache.commons.io.IOUtils;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.doc.user.generator.RoadAndRoadUseWithTimesAndCardinalitiesElementGenerator;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.jobtracker.JobStatus;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.job.GetAllJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobResults;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;

public class Jobs extends DevWalkthrough {
    public Jobs() {
        super("Jobs", "RoadAndRoadUseWithTimesAndCardinalities");
    }

    public CloseableIterable<? extends Element> run() throws OperationException, IOException {
        /// [graph] create a graph using our schema and store properties
        // ---------------------------------------------------------
        final Graph graph = new Graph.Builder()
                .config(StreamUtil.graphConfig(getClass()))
                .addSchemas(StreamUtil.openStreams(getClass(), "RoadAndRoadUseWithTimesAndCardinalities/schema"))
                .storeProperties(StreamUtil.openStream(getClass(), "mockaccumulostore.properties"))
                .build();
        // ---------------------------------------------------------


        // [user] Create a user
        // ---------------------------------------------------------
        final User user = new User("user01");
        // ---------------------------------------------------------


        // [add] Create a data generator and add the edges to the graph using an operation chain consisting of:
        // generateElements - generating edges from the data (note these are directed edges)
        // addElements - add the edges to the graph
        // ---------------------------------------------------------
        final OperationChain<Void> addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .generator(new RoadAndRoadUseWithTimesAndCardinalitiesElementGenerator())
                        .input(IOUtils.readLines(StreamUtil.openStream(getClass(), "RoadAndRoadUseWithTimesAndCardinalities/data.txt")))
                        .build())
                .then(new AddElements())
                .build();

        graph.execute(addOpChain, user);
        // ---------------------------------------------------------


        // [job] create an operation chain to be executed as a job
        // ---------------------------------------------------------
        final OperationChain<CloseableIterable<? extends Element>> job = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed("10"))
                        .view(new View.Builder()
                                .edge("RoadUse")
                                .build())
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

        return (CloseableIterable) jobResults;
    }

    private void waitUntilJobHashFinished(final User user, final Graph graph, final JobDetail initialJobDetail) throws OperationException {
        JobDetail jobDetail = initialJobDetail;
        while (JobStatus.RUNNING.equals(jobDetail.getStatus())) {
            jobDetail = graph.execute(new GetJobDetails.Builder()
                    .jobId(jobDetail.getJobId())
                    .build(), user);
            try {
                Thread.sleep(100);
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(final String[] args) throws OperationException, IOException {
        final Jobs walkthrough = new Jobs();
        walkthrough.run();
    }
}
