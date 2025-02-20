/*
 * Copyright 2024-2025 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple.operation.handler;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.federated.simple.FederatedUtils;
import uk.gov.gchq.gaffer.federated.simple.access.GraphAccess;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Main default handler for federated operations. Handles delegation to selected
 * graphs and will sub class the operation to a {@link FederatedOutputHandler}
 * if provided operation has output so that it is merged.
 */
public class FederatedOperationHandler<P extends Operation> implements OperationHandler<P> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederatedOperationHandler.class);

    /**
     * The operation option for the Graph IDs that an operation should be
     * executed on, will take preference over the short variant of this option.
     */
    public static final String OPT_GRAPH_IDS = "gaffer.federatedstore.operation.graphIds";

    /**
     * The short version of the operation option for the Graph IDs that an
     * operation should be executed on.
     */
    public static final String OPT_SHORT_GRAPH_IDS = "federated.graphIds";

    /**
     * Graph IDs to exclude from the execution. If this option is set all graphs
     * except the ones specified are executed on.
     */
    public static final String OPT_EXCLUDE_GRAPH_IDS = "federated.excludeGraphIds";

    /**
     * A boolean option to specify to use the default graph IDs. The option is
     * not specifically required as default graph IDs will be used as a
     * fallback, but if set the whole chain will be forwarded rather than each
     * individual operation so can speed things up.
     */
    public static final String OPT_USE_DFLT_GRAPH_IDS = "federated.useDefaultGraphIds";

    /**
     * The boolean operation option to specify if element merging should be applied or not.
     */
    public static final String OPT_AGGREGATE_ELEMENTS = "federated.aggregateElements";

    /**
     * A boolean option to specify if a graph should be skipped if execution
     * fails on it e.g. continue executing on the rest of the graphs
     */
    public static final String OPT_SKIP_FAILED_EXECUTE = "federated.skipGraphOnFail";

    /**
     * A boolean option to specify if the results from each graph should be kept
     * separate. If set this will return a map where each key value is the graph
     * ID and its respective result.
     */
    public static final String OPT_SEPARATE_RESULTS = "federated.separateResults";

    /**
     * Depth should go to when making an operation chain relevant to specified
     * graphs e.g. fix the View.
     */
    public static final String OPT_FIX_OP_LIMIT = "federated.fixOperationLimit";

    /**
     * Default depth limit for fixing an operation chain.
     */
    public static final int DFLT_FIX_OP_LIMIT = 5;

    @Override
    public Object doOperation(final P operation, final Context context, final Store store) throws OperationException {
        LOGGER.debug("Running operation: {}", operation);
        final int fixLimit = Integer.parseInt(operation.getOption(OPT_FIX_OP_LIMIT, String.valueOf(DFLT_FIX_OP_LIMIT)));

        // If the operation has output wrap and return using sub class handler
        if (operation instanceof Output) {
            // Should we keep the results separate
            if (Boolean.parseBoolean(operation.getOption(OPT_SEPARATE_RESULTS, "false"))) {
                return new SeparateOutputHandler<>().doOperation((Output)  operation, context, store);
            }
            return new FederatedOutputHandler<>().doOperation((Output) operation, context, store);
        }

        List<GraphSerialisable> graphsToExecute = getGraphsToExecuteOn(operation, context, (FederatedStore) store);
        // No-op
        if (graphsToExecute.isEmpty()) {
            return null;
        }

        // Execute the operation chain on each graph
        for (final GraphSerialisable gs : graphsToExecute) {
            try {
                gs.getGraph().execute(
                    FederatedUtils.getValidOperationForGraph(operation, gs, 0, fixLimit),
                    context.getUser());
            } catch (final OperationException | UnsupportedOperationException | IllegalArgumentException e) {
                // Optionally skip this error if user has specified to do so
                LOGGER.error("Operation failed on graph: {}", gs.getGraphId());
                if (!Boolean.parseBoolean(operation.getOption(OPT_SKIP_FAILED_EXECUTE, "false"))) {
                    throw e;
                }
                LOGGER.info("Continuing operation execution on sub graphs");
            }
        }

        // Assume no output, we've already checked above
        return null;
    }


    /**
     * Extract the graph IDs from the operation and process the option.
     * Will default to the store configured graph IDs if no option present.
     * <p>
     * Returned list will be ordered alphabetically based on graph ID for
     * predicability.
     *
     * @param operation The operation to execute.
     * @param context The context.
     * @param store The federated store.
     * @return List of {@link GraphSerialisable}s to execute on.
     * @throws OperationException Fail to get Graphs.
     */
    protected List<GraphSerialisable> getGraphsToExecuteOn(final Operation operation, final Context context,
            final FederatedStore store) throws OperationException {
        List<String> specifiedGraphIds = new ArrayList<>();
        List<GraphSerialisable> graphsToExecute = new ArrayList<>();

        // If user specified graph IDs for this chain parse as comma separated list
        if (operation.containsOption(OPT_SHORT_GRAPH_IDS)) {
            specifiedGraphIds.addAll(Arrays.asList(operation.getOption(OPT_SHORT_GRAPH_IDS).split(",")));
        // Check legacy option
        } else if (operation.containsOption(OPT_GRAPH_IDS)) {
            specifiedGraphIds.addAll(Arrays.asList(operation.getOption(OPT_GRAPH_IDS).split(",")));
        // If a user has specified to just exclude some graphs then run all but them
        } else if (operation.containsOption(OPT_EXCLUDE_GRAPH_IDS)) {
            store.getAllGraphsAndAccess().forEach(pair -> specifiedGraphIds.add(pair.getLeft().getGraphId()));
            // Exclude the ones the user has specified
            Arrays.asList(operation.getOption(OPT_EXCLUDE_GRAPH_IDS).split(",")).forEach(specifiedGraphIds::remove);
        }

        // Use default graph IDs as a fallback
        if (specifiedGraphIds.isEmpty()) {
            specifiedGraphIds.addAll(store.getDefaultGraphIds());
        }

        // Get the corresponding graph serialisables
        for (final String id : specifiedGraphIds) {
            try {
                Pair<GraphSerialisable, GraphAccess> pair = store.getGraphAccessPair(id);

                // Check the user has access to the graph
                if (pair.getRight().hasReadAccess(context.getUser(), store.getProperties().getAdminAuth())) {
                    LOGGER.debug("User has access, will execute on Graph: '{}'", id);
                    // Create a new graph object from the serialised info
                    graphsToExecute.add(pair.getLeft());
                } else {
                    LOGGER.warn("User does not have access, to Graph: '{}' it will be skipped", id);
                }
            } catch (final CacheOperationException e) {
                throw new OperationException("Failed to get Graph from cache: '" + id + "'", e);
            }
        }

        // Keep graphs sorted so results returned are predictable between runs
        Collections.sort(graphsToExecute, (g1, g2) -> g1.getGraphId().compareTo(g2.getGraphId()));

        return graphsToExecute;
    }
}
