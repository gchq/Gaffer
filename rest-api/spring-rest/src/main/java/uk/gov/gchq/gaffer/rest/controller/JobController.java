/*
 * Copyright 2020 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.controller;

import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.jobtracker.Job;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.job.GetAllJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobResults;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UserFactory;

import java.net.URI;

import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.JOB_ID_HEADER;

@RestController
public class JobController implements IJobController {

    private GraphFactory graphFactory;
    private UserFactory userFactory;

    @Autowired
    public void setGraphFactory(final GraphFactory graphFactory) {
        this.graphFactory = graphFactory;
    }

    @Autowired
    public void setUserFactory(final UserFactory userFactory) {
        this.userFactory = userFactory;
    }

    @Override
    public ResponseEntity<JobDetail> startJob(@RequestBody final Operation operation) throws OperationException {
        JobDetail jobDetail = graphFactory.getGraph().executeJob(OperationChain.wrap(operation), userFactory.createContext());
        URI jobUri = ServletUriComponentsBuilder.fromCurrentRequest().pathSegment(jobDetail.getJobId()).build().toUri();
        return ResponseEntity.created(jobUri)
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .header(JOB_ID_HEADER, jobDetail.getJobId())
                .body(jobDetail);
    }

    @Override
    public ResponseEntity<JobDetail> scheduleJob(@RequestBody final Job job) throws OperationException {
        JobDetail jobDetail = graphFactory.getGraph().executeJob(job, userFactory.createContext());
        URI jobUri = ServletUriComponentsBuilder.fromCurrentRequest().pathSegment(jobDetail.getJobId()).build().toUri();
        return ResponseEntity.created(jobUri)
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .header(JOB_ID_HEADER, jobDetail.getJobId())
                .body(jobDetail);
    }

    @Override
    public ResponseEntity<JobDetail> getDetails(@PathVariable("id") @ApiParam("The Job ID") final String id) throws OperationException {
        JobDetail jobDetail = graphFactory.getGraph().execute(new GetJobDetails.Builder()
                        .jobId(id)
                        .build(),
                userFactory.createContext()
        );

        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .body(jobDetail);
    }

    @Override
    public ResponseEntity<Iterable<JobDetail>> getAllDetails() throws OperationException {
        CloseableIterable<JobDetail> jobDetails = graphFactory.getGraph().execute(new GetAllJobDetails(), userFactory.createContext());

        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .body(jobDetails);
    }

    @Override
    public ResponseEntity<Object> getResults(@PathVariable("id") @ApiParam("The Job ID") final String id) throws OperationException {
        CloseableIterable<?> results = graphFactory.getGraph().execute(new GetJobResults.Builder()
                        .jobId(id)
                        .build(),
                userFactory.createContext()
        );

        return ResponseEntity.ok()
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .body(results);
    }
}
