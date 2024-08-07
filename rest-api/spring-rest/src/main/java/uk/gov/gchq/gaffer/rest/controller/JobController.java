/*
 * Copyright 2020-2024 Crown Copyright
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

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import uk.gov.gchq.gaffer.jobtracker.Job;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.job.GetAllJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobResults;
import uk.gov.gchq.gaffer.rest.factory.GraphFactory;
import uk.gov.gchq.gaffer.rest.factory.spring.AbstractUserFactory;

import java.net.URI;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.JOB_ID_HEADER;

@RestController
@Tag(name = "job")
@RequestMapping("/rest/graph/jobs")
public class JobController {

    private final GraphFactory graphFactory;
    private final AbstractUserFactory userFactory;

    @Autowired
    public JobController(final GraphFactory graphFactory, final AbstractUserFactory userFactory) {
        this.graphFactory = graphFactory;
        this.userFactory = userFactory;
    }

    @PostMapping(consumes = APPLICATION_JSON_VALUE, produces = APPLICATION_JSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(summary = "Kicks off an asynchronous job")
    public ResponseEntity<JobDetail> startJob(
            @RequestHeader final HttpHeaders httpHeaders,
            @RequestBody final Operation operation) throws OperationException {
        userFactory.setHttpHeaders(httpHeaders);
        final JobDetail jobDetail = graphFactory.getGraph().executeJob(OperationChain.wrap(operation), userFactory.createContext());
        final URI jobUri = ServletUriComponentsBuilder.fromCurrentRequest().pathSegment(jobDetail.getJobId()).build().toUri();
        return ResponseEntity.created(jobUri)
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .header(JOB_ID_HEADER, jobDetail.getJobId())
                .body(jobDetail);
    }

    @PostMapping(path = "/schedule", consumes = APPLICATION_JSON_VALUE, produces = APPLICATION_JSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(summary = "Schedules an asynchronous job")
    public ResponseEntity<JobDetail> scheduleJob(
            @RequestHeader final HttpHeaders httpHeaders,
            @RequestBody final Job job) throws OperationException {
        userFactory.setHttpHeaders(httpHeaders);
        final JobDetail jobDetail = graphFactory.getGraph().executeJob(job, userFactory.createContext());
        final URI jobUri = ServletUriComponentsBuilder.fromCurrentRequest().pathSegment(jobDetail.getJobId()).build().toUri();
        return ResponseEntity.created(jobUri)
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .header(JOB_ID_HEADER, jobDetail.getJobId())
                .body(jobDetail);
    }

    @GetMapping(path = "/{id}", produces = APPLICATION_JSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(summary = "Retrieves the details of an asynchronous job")
    public JobDetail getDetails(
            @RequestHeader final HttpHeaders httpHeaders,
            @PathVariable("id") @Parameter(description = "The Job ID") final String id) throws OperationException {
        userFactory.setHttpHeaders(httpHeaders);
        return graphFactory.getGraph().execute(new GetJobDetails.Builder()
                        .jobId(id)
                        .build(),
                userFactory.createContext()
        );
    }

    @GetMapping(produces = APPLICATION_JSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(summary = "Retrieves the details of all the asynchronous jobs")
    public Iterable<JobDetail> getAllDetails(@RequestHeader final HttpHeaders httpHeaders) throws OperationException {
        userFactory.setHttpHeaders(httpHeaders);
        return graphFactory.getGraph().execute(new GetAllJobDetails(), userFactory.createContext());
    }

    @GetMapping(path = "/{id}/results", produces = APPLICATION_JSON_VALUE)
    @io.swagger.v3.oas.annotations.Operation(summary = "Retrieves the results of an asynchronous job")
    public Object getResults(
            @RequestHeader final HttpHeaders httpHeaders,
            @PathVariable("id") @Parameter(description = "The Job ID") final String id) throws OperationException {
        userFactory.setHttpHeaders(httpHeaders);
        return graphFactory.getGraph().execute(new GetJobResults.Builder()
                        .jobId(id)
                        .build(),
                userFactory.createContext()
        );
    }
}
