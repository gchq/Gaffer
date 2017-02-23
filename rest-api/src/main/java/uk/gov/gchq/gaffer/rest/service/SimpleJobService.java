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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.rest.GraphFactory;
import uk.gov.gchq.gaffer.store.operation.GetAllJobStatuses;
import uk.gov.gchq.gaffer.store.operation.GetJobStatus;
import uk.gov.gchq.gaffer.user.User;

/**
 * An implementation of {@link IJobService}. By default it will use a singleton
 * {@link Graph} generated using the {@link GraphFactory}.
 * All operations are simply delegated to the graph.
 * Pre and post operation hooks are available by extending this class and implementing preOperationHook and/or
 * postOperationHook.
 * <p>
 * By default queries will be executed with an UNKNOWN user containing no auths.
 * The createUser() method should be overridden and a {@link User} object should
 * be created from the http request.
 * </p>
 */
public class SimpleJobService implements IJobService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleJobService.class);
    private final GraphFactory graphFactory;

    public SimpleJobService() {
        this(GraphFactory.createGraphFactory());
    }

    public SimpleJobService(final GraphFactory graphFactory) {
        this.graphFactory = graphFactory;
    }

    @Override
    public JobDetail executeJob(final OperationChain opChain) {
        final User user = createUser();
        preOperationHook(opChain, user);

        try {
            final JobDetail jobDetail = graphFactory.getGraph().executeJob(opChain, user);
            LOGGER.info("Job started = " + jobDetail);
            return jobDetail;
        } catch (OperationException e) {
            throw new RuntimeException("Error executing opChain", e);
        } finally {
            postOperationHook(opChain, user);
        }
    }

    @Override
    public CloseableIterable<JobDetail> status() {
        try {
            return getGraph().execute(new GetAllJobStatuses(), createUser());
        } catch (OperationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public JobDetail status(final String id) {
        try {
            return getGraph().execute(
                    new GetJobStatus.Builder()
                            .jobId(id)
                            .build(),
                    createUser());
        } catch (OperationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a {@link User} object containing information about the user
     * querying Gaffer.
     * By default this will return a user with id: UNKNOWN.
     * <p>
     * This method should be overridden for implementations of this API. The
     * user information should be fetched from the request.
     *
     * @return the user querying Gaffer.
     */
    protected User createUser() {
        return new User();
    }

    protected void preOperationHook(final OperationChain<?> opChain, final User user) {
        // no action by default
    }

    protected void postOperationHook(final OperationChain<?> opChain, final User user) {
        // no action by default
    }

    protected Graph getGraph() {
        return graphFactory.getGraph();
    }
}
