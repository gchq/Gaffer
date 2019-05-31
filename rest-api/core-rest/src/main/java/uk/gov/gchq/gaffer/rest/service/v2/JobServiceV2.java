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

package uk.gov.gchq.gaffer.rest.service.v2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import uk.gov.gchq.gaffer.store.Context;

import javax.inject.Inject;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.net.URI;

import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.GAFFER_MEDIA_TYPE_HEADER;
import static uk.gov.gchq.gaffer.rest.ServiceConstants.JOB_ID_HEADER;

/**
 * An implementation of {@link IJobServiceV2}. By default it will use a singleton
 * {@link uk.gov.gchq.gaffer.graph.Graph} generated using the {@link uk.gov.gchq.gaffer.rest.factory.GraphFactory}.
 * All operations are simply delegated to the graph.
 * Pre and post operation hooks are available by extending this class and implementing preOperationHook and/or
 * postOperationHook.
 */
public class JobServiceV2 implements IJobServiceV2 {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobServiceV2.class);

    @Inject
    private GraphFactory graphFactory;

    @Inject
    private UserFactory userFactory;

    @javax.ws.rs.core.Context
    private UriInfo uriInfo;

    @Override
    public Response executeJob(final Operation operation) throws OperationException {
        final Context context = userFactory.createContext();

        final OperationChain opChain = OperationChain.wrap(operation);

        preOperationHook(opChain, context);

        try {
            final JobDetail jobDetail = graphFactory.getGraph()
                    .executeJob(opChain, context);
            LOGGER.info("Job started = {}", jobDetail);

            final URI location = uriInfo.getAbsolutePathBuilder()
                    .path(jobDetail.getJobId())
                    .build();

            return Response.created(location).entity(jobDetail)
                    .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                    .header(JOB_ID_HEADER, context.getJobId())
                    .build();
        } finally {
            postOperationHook(opChain, context);
        }
    }

    @Override
    public Response executeJob(final Job job) throws OperationException {
        final Context context = userFactory.createContext();
        OperationChain chain = OperationChain.wrap(job.getOperation());
        preOperationHook(chain, context);

        try {
            final JobDetail jobDetail = graphFactory.getGraph().executeJob(job, context);
            LOGGER.info("Job started = {}", jobDetail);

            final URI location = uriInfo.getAbsolutePathBuilder()
                    .path(jobDetail.getJobId())
                    .build();

            return Response.created(location).entity(jobDetail)
                    .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                    .header(JOB_ID_HEADER, context.getJobId())
                    .build();
        } finally {
            postOperationHook(chain, context);
        }
    }

    @Override
    public Response details() throws OperationException {
        final Context context = userFactory.createContext();
        return Response.ok(graphFactory.getGraph()
                .execute(new GetAllJobDetails(),
                        context))
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .header(JOB_ID_HEADER, context.getJobId())
                .build();
    }

    @Override
    public Response details(final String id) throws OperationException {
        final Context context = userFactory.createContext();
        return Response.ok(graphFactory.getGraph().execute(
                new GetJobDetails.Builder()
                        .jobId(id)
                        .build(),
                context))
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .header(JOB_ID_HEADER, context.getJobId())
                .build();
    }

    @Override
    public Response results(final String id) throws OperationException {
        final Context context = userFactory.createContext();
        return Response.ok(graphFactory.getGraph().execute(
                new GetJobResults.Builder()
                        .jobId(id)
                        .build(),
                context))
                .header(GAFFER_MEDIA_TYPE_HEADER, GAFFER_MEDIA_TYPE)
                .header(JOB_ID_HEADER, context.getJobId())
                .build();
    }

    protected void preOperationHook(final OperationChain<?> opChain, final Context context) {
        // no action by default
    }

    protected void postOperationHook(final OperationChain<?> opChain, final Context context) {
        // no action by default
    }
}
