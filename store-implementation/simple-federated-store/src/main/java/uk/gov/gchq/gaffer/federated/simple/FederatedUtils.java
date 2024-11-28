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
import java.util.stream.Collectors;

/**
 * Utility class with static methods to help support the federated store.
 */
public final class FederatedUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederatedUtils.class);

    private FederatedUtils() {
        // utility class
    }

    /**
     * Checks if graphs share any groups between their schemas.
     *
     * @param graphs Graphs to check.
     * @return Do they share groups.
     */
    public static boolean doGraphsShareGroups(final List<GraphSerialisable> graphs) {
        // Check if any of the graphs have common groups in their schemas
        List<Schema> schemas = graphs.stream()
            .map(GraphSerialisable::getSchema)
            .collect(Collectors.toList());

        // Compare all schemas against each other
        for (int i = 0; i < schemas.size() - 1; i++) {
            for (int j = i + 1; j < schemas.size(); j++) {
                // Compare each schema against the others to see if common groups
                if (!Collections.disjoint(schemas.get(i).getGroups(), schemas.get(j).getGroups())) {
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
     * @return A valid version of the operation chain.
     */
    public static OperationChain getValidOperationForGraph(final Operation operation, final GraphSerialisable graphSerialisable, final int depth) {
        LOGGER.debug("Creating valid operation for graph, depth is: {}", depth);
        final Collection<Operation> updatedOperations = new ArrayList<>();

        final Schema schema = graphSerialisable.getSchema();

        // Fix the view so it will pass schema validation
        if (operation instanceof OperationView
                && ((OperationView) operation).getView() != null
                && ((OperationView) operation).getView().hasGroups()) {

            View view = ((OperationView) operation).getView();
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
                ((OperationView) operation).setView(builder.build());
            }
            updatedOperations.add(operation);

        // Recursively go into operation chains to make sure everything is fixed
        } else if (operation instanceof OperationChain) {
            for (final Operation op : ((OperationChain<?>) operation).getOperations()) {
                // Resolve if haven't hit the depth limit for validation
                if (depth < 5) {
                    updatedOperations.addAll(getValidOperationForGraph(op, graphSerialisable, depth + 1).getOperations());
                } else {
                    LOGGER.warn("Hit depth limit of 5 whilst creating a valid View. The View may be invalid for Graph: {}", graphSerialisable.getGraphId());
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


}
