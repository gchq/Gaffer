/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.FederatedOperationHandler;
import uk.gov.gchq.gaffer.federated.simple.operation.handler.get.GetSchemaHandler;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility class with static methods to help support the federated store.
 */
public final class FederatedUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederatedUtils.class);

    private FederatedUtils() {
        // utility class
    }

    /**
     * Gets a merged schema based on the graphs specified.
     *
     * @param graphs The graphs to get the schemas from.
     * @return A merged {@link Schema}
     */
    public static Schema getSchema(final List<GraphSerialisable> graphs) {
        List<Schema> schemasToMerge = new ArrayList<>();
        graphs.forEach(g -> schemasToMerge.add(g.getSchema()));
        return GetSchemaHandler.getMergedSchema(schemasToMerge);
    }

    /**
     * Checks if graphs share any groups between their schemas.
     *
     * @param graphs Graphs to check.
     * @return Do they share groups.
     */
    public static boolean doGraphsShareGroups(final List<GraphSerialisable> graphs) {
        // Compare all schemas against each other
        for (int i = 0; i < graphs.size() - 1; i++) {
            for (int j = i + 1; j < graphs.size(); j++) {
                // Compare each schema against the others to see if common groups
                if (!Collections.disjoint(graphs.get(i).getSchema().getGroups(), graphs.get(j).getSchema().getGroups())) {
                    LOGGER.debug("Found common schema groups between requested graphs");
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Get a version of the operation chain that satisfies the schema for the
     * requested graph. This will ensure the operation will pass validation on
     * the sub graph end as there is no universal way to skip validation.
     *
     * @param operation The operation.
     * @param graphSerialisable The graph.
     * @param depth Current recursion depth of this method.
     * @param depthLimit Limit to the recursion depth.
     * @return A valid version of the operation chain.
     */
    public static OperationChain getValidOperationForGraph(final Operation operation, final GraphSerialisable graphSerialisable, final int depth, final int depthLimit) {
        LOGGER.debug("Creating valid operation for graph, depth is: {}", depth);
        final Collection<Operation> updatedOperations = new ArrayList<>();

        // Fix the view so it will pass schema validation
        if (operation instanceof OperationView
                && ((OperationView) operation).getView() != null
                && ((OperationView) operation).getView().hasGroups()) {

            // Update the view for the graph
            ((OperationView) operation).setView(
                getValidViewForGraph(((OperationView) operation).getView(), graphSerialisable));

            updatedOperations.add(operation);

        // Recursively go into operation chains to make sure everything is fixed
        } else if (operation instanceof OperationChain) {
            for (final Operation op : ((OperationChain<?>) operation).getOperations()) {
                // Resolve if haven't hit the depth limit for validation
                if (depth < depthLimit) {
                    updatedOperations.addAll(getValidOperationForGraph(op, graphSerialisable, depth + 1, depthLimit).getOperations());
                } else {
                    LOGGER.warn(
                        "Hit depth limit of {} making the operation valid for graph. The View may be invalid for Graph: {}",
                        depthLimit,
                        graphSerialisable.getGraphId());
                    updatedOperations.add(op);
                }
            }
        } else {
            updatedOperations.add(operation);
        }

        // Create and return the fixed chain for the graph
        OperationChain<?> newChain = new OperationChain<>();
        newChain.setOptions(operation.getOptions());
        newChain.updateOperations(updatedOperations);

        return newChain;
    }

    /**
     * Returns a {@link View} that contains groups only relevant to the graph. If
     * the supplied view does not require modification it will just be returned.
     *
     * @param view The view to make valid.
     * @param graphSerialisable The relevant graph.
     * @return A version of the view valid for the graph.
     */
    public static View getValidViewForGraph(final View view, final GraphSerialisable graphSerialisable) {
        final Schema schema = graphSerialisable.getSchema();

        // Figure out all the groups relevant to the graph
        final Set<String> validEntities = new HashSet<>(view.getEntityGroups());
        final Set<String> validEdges = new HashSet<>(view.getEdgeGroups());
        validEntities.retainAll(schema.getEntityGroups());
        validEdges.retainAll(schema.getEdgeGroups());

        if (!validEntities.equals(view.getEntityGroups()) || !validEdges.equals(view.getEdgeGroups())) {
            // Need to make changes to the view so start by cloning the view
            // and clearing all the edges and entities
            final View.Builder builder = new View.Builder()
                .merge(view)
                .entities(Collections.emptyMap())
                .edges(Collections.emptyMap());
            validEntities.forEach(e -> builder.entity(e, view.getEntity(e)));
            validEdges.forEach(e -> builder.edge(e, view.getEdge(e)));
            final View newView = builder.build();
            // If the View has no groups left after fixing then this is likely an issue so throw
            if (!newView.hasEntities() && !newView.hasEdges()) {
                throw new IllegalArgumentException(String.format(
                    "No groups specified in View are relevant to Graph: '%1$s'. " +
                    "Please refine your Graphs/View or specify following option to skip execution on offending Graph: '%2$s'   ",
                    graphSerialisable.getGraphId(),
                    FederatedOperationHandler.OPT_SKIP_FAILED_EXECUTE));
            }
            return newView;
        }

        // Nothing to do return unmodified view
        return view;
    }

}
