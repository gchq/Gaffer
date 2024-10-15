/*
 * Copyright 2022-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.util;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.io.Closeable;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ApplyViewToElementsFunction implements ContextSpecificMergeFunction<Object, Iterable<Object>, Iterable<Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApplyViewToElementsFunction.class);
    public static final String VIEW = "view";
    public static final String SCHEMA = "schema";
    public static final String USER = "user";
    public static final String TEMP_RESULTS_GRAPH = "temporaryResultsGraph";
    private static final SecureRandom RANDOM = new SecureRandom();

    @JsonProperty("context")
    private Map<String, Object> context;

    public ApplyViewToElementsFunction() {
    }

    public ApplyViewToElementsFunction(final Map<String, Object> context) throws GafferCheckedException {
        this();
        try {
            // Check if results graph, hasn't already be supplied, otherwise make a default results graph.
            if (!context.containsKey(TEMP_RESULTS_GRAPH)) {
                final Graph resultsGraph = new Graph.Builder()
                        .config(new GraphConfig(String.format("%s%s%d", TEMP_RESULTS_GRAPH, ApplyViewToElementsFunction.class.getSimpleName(), RANDOM.nextInt(Integer.MAX_VALUE))))
                        .addSchema((Schema) context.get(SCHEMA))
                        //MapStore easy in memory Store. Large results size may not be suitable, a graph could be provided via Context.
                        .addStoreProperties(new MapStoreProperties())
                        .build();

                LOGGER.debug("A Temporary results graph named:{} is being made with schema:{}", resultsGraph.getGraphId(), resultsGraph.getSchema());

                context.put(TEMP_RESULTS_GRAPH, resultsGraph);
            }
            // Validate the supplied context before using
            validate(context);
            this.context = Collections.unmodifiableMap(context);
        } catch (final Exception e) {
            throw new GafferCheckedException("Unable to create TemporaryResultsGraph", e);
        }

    }

    @Override
    public ApplyViewToElementsFunction createFunctionWithContext(final HashMap<String, Object> context) throws GafferCheckedException {
        return new ApplyViewToElementsFunction(context);
    }

    /**
     * Validates the supplied context to ensure we have everything needed to run the Function
     *
     * @param context The context e.g. view, schema and user
     */
    private static void validate(final Map<String, Object> context) {
        View view = (View) context.get(VIEW);
        if (view != null && view.hasTransform()) {
            throw new UnsupportedOperationException("Error: context invalid: can not use this function with a POST AGGREGATION TRANSFORM VIEW, " +
                    "because transformation may have created items that does not exist in the schema. " +
                    "The re-applying of the View to the collected federated results would not be be possible. " +
                    "Try a simple concat merge that doesn't require the re-application of view");
            // Solution is to derive and use the "Transformed schema" from the uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition.
        }

        Schema schema = (Schema) context.get(SCHEMA);
        if (schema == null || !schema.hasGroups()) {
            throw new IllegalArgumentException("Error: context invalid, requires a populated schema.");
        }

        if (!context.containsKey(TEMP_RESULTS_GRAPH)) {
            throw new IllegalStateException("Error: context invalid, did not contain a Temporary Results Graph.");
        } else if (!(context.get(TEMP_RESULTS_GRAPH) instanceof Graph)) {
            throw new IllegalArgumentException(String.format("Error: context invalid, value for %s was not a Graph, found: %s", TEMP_RESULTS_GRAPH, context.get(TEMP_RESULTS_GRAPH)));
        }

        if (!context.containsKey(USER)) {
            throw new IllegalArgumentException("Error: context invalid, requires a User");
        }
    }

    @Override
    @JsonIgnore
    public Set<String> getRequiredContextValues() {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(VIEW, SCHEMA, USER)));
    }

    @Override
    public Iterable<Object> apply(final Object update, final Iterable<Object> state) {
        if (state instanceof Closeable) {
            Closeable closeable = (Closeable) state;
            try {
                closeable.close();
            } catch (final IOException e) {
                LOGGER.error("Error closing looped iterable", e);
            }
        }

        final Graph resultsGraph = (Graph) context.get(TEMP_RESULTS_GRAPH);
        final Context userContext = new Context((User) context.get(USER));
        try {
            // The update object might be a lazy AccumuloElementRetriever and might be MASSIVE.
            resultsGraph.execute(new AddElements.Builder().input((Iterable<Element>) update).build(), userContext);
        } catch (final OperationException e) {
            throw new GafferRuntimeException("Error adding elements to temporary results graph, due to:" + e.getMessage(), e);
        }

        try {
            return (Iterable) resultsGraph.execute(new GetAllElements.Builder().view((View) context.get(VIEW)).build(), userContext);
        } catch (final OperationException e) {
            throw new GafferRuntimeException("Error getting all elements from temporary graph, due to:" + e.getMessage(), e);
        }
    }
}
