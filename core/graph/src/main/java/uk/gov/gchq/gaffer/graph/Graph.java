/*
 * Copyright 2017-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.graph;

import uk.gov.gchq.gaffer.graph.util.GraphConfig;
import uk.gov.gchq.gaffer.graph.util.GraphRequest;
import uk.gov.gchq.gaffer.graph.util.GraphResult;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.util.Request;
import uk.gov.gchq.gaffer.store.util.Result;
import uk.gov.gchq.gaffer.user.User;

/**
 * A Graph provides a simple Java API for querying Gaffer Stores.
 * An initialiseStore method can be used to create a Graph Store instance
 * which can then be queried using this API.
 * <p>
 * A Graph contains nothing and only forwards queries to the supplied Store
 * instance.
 */
public final class Graph {

    /**
     * private constructor to prevent instantiation.
     */
    private Graph() {
    }

    /**
     * Creates a new Store instance using the GraphConfig supplied.
     *
     * @param config the {@link GraphConfig} instance.
     * @return the Store instance created.
     */
    public static Store initialiseGraphStore(final GraphConfig config) {
        return Store.createStore(config);
    }

    /**
     * Performs the given operation on the store.
     * If the operation does not have a view then the graph view is used.
     *
     * @param operation the operation to be executed.
     * @param user      the user executing the operation.
     * @throws OperationException if an operation fails
     */
    public static void execute(final Operation operation, final User user, final Store store) throws OperationException {
        execute(new GraphRequest<>(operation, user), store);
    }

    /**
     * Performs the given operation on the store.
     * If the operation does not have a view then the graph view is used.
     * The context will be cloned and a new jobId will be created.
     *
     * @param operation the operation to be executed.
     * @param context   the user context for the execution of the operation
     * @throws OperationException if an operation fails
     */
    public static void execute(final Operation operation, final Context context, final Store store) throws OperationException {
        execute(new Request<>(operation, context), store);
    }

    /**
     * Performs the given output operation on the store.
     * If the operation does not have a view then the graph view is used.
     *
     * @param operation the output operation to be executed.
     * @param user      the user executing the operation.
     * @param <O>       the operation chain output type.
     * @return the operation result.
     * @throws OperationException if an operation fails
     */
    public static <O> O execute(final Output<O> operation, final User user, final Store store) throws OperationException {
        return execute(new Request<>(operation, user), store).getResult();
    }

    /**
     * Performs the given output operation on the store.
     * If the operation does not have a view then the graph view is used.
     * The context will be cloned and a new jobId will be created.
     *
     * @param operation the output operation to be executed.
     * @param context   the user context for the execution of the operation.
     * @param <O>       the operation chain output type.
     * @return the operation result.
     * @throws OperationException if an operation fails
     */
    public static <O> O execute(final Output<O> operation, final Context context, final Store store) throws OperationException {
        return execute(new Request<>(operation, context), store).getResult();
    }

    /**
     * Executes a {@link GraphRequest} on the graph and returns the
     * {@link GraphResult}.
     *
     * @param request the request to execute
     * @param <O>     the result type
     * @return {@link GraphResult} containing the result and details
     * @throws OperationException if an operation fails
     */
    public static <O> Result<O> execute(final Request<O> request, final Store store) throws OperationException {
        return _execute(store::execute, request);
    }

    /**
     * Performs the given operation job on the store.
     * If the operation does not have a view then the graph view is used.
     *
     * @param operation the operation to be executed.
     * @param user      the user executing the job.
     * @return the job details
     * @throws OperationException thrown if the job fails to run.
     */
    public static JobDetail executeJob(final Operation operation, final User user, final Store store) throws OperationException {
        return executeJob(new GraphRequest<>(operation, user), store).getResult();
    }

    /**
     * Performs the given operation job on the store.
     * If the operation does not have a view then the graph view is used.
     * The context will be cloned and a new jobId will be created.
     *
     * @param operation the operation to be executed.
     * @param context   the user context for the execution of the operation
     * @return the job details
     * @throws OperationException thrown if the job fails to run.
     */
    public static JobDetail executeJob(final Operation operation, final Context context, final Store store) throws OperationException {
        return executeJob(new GraphRequest<>(operation, context), store).getResult();
    }

    /**
     * Executes the given {@link GraphRequest} on the graph as an asynchronous job
     * and returns a {@link GraphResult} containing the {@link JobDetail}s.
     * If the operation does not have a view then the graph view is used.
     * The context will be cloned and a new jobId will be created.
     *
     * @param request the request to execute
     * @return {@link GraphResult} containing the job details
     * @throws OperationException thrown if the job fails to run.
     */
    public static GraphResult<JobDetail> executeJob(final Request<?> request, final Store store) throws OperationException {
        return _execute(store::executeJob, request);
    }

    private static <O> GraphResult<O> _execute(final StoreExecuter<O> storeExecuter, final Request<?> request) throws OperationException {
        return (GraphResult<O>) storeExecuter.execute(request);
    }

    @FunctionalInterface
    private interface StoreExecuter<O> {
        Result<O> execute(final Request request) throws OperationException;
    }
}