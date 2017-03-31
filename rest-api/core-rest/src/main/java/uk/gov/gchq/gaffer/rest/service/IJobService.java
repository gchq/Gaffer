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

package uk.gov.gchq.gaffer.rest.service;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.operation.OperationChain;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * An <code>IJobService</code> handles jobs - executing Jobs and getting Job
 * statuses.
 */
@Path("/graph/jobs")
@Api(value = "job", description = "Allows jobs to be executed on the graph. See <a href='https://github.com/gchq/Gaffer/wiki/operation-examples' target='_blank'>Wiki</a>.")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface IJobService {

    @POST
    @Path("/doOperation")
    @ApiOperation(value = "Performs the given operation chain job on the graph", response = JobDetail.class)
    JobDetail executeJob(final OperationChain opChain);

    @GET
    @ApiOperation(value = "Get the details of all jobs", response = JobDetail.class, responseContainer = "List")
    CloseableIterable<JobDetail> details();

    @GET
    @Path("{id}")
    @ApiOperation(value = "Get the details of a job", response = JobDetail.class)
    JobDetail details(@ApiParam(value = "a job id") @PathParam("id") final String id);

    @GET
    @Path("{id}/results")
    @ApiOperation(value = "Get the results of a job", response = Object.class, responseContainer = "List")
    CloseableIterable results(@ApiParam(value = "a job id") @PathParam("id") final String id);
}
