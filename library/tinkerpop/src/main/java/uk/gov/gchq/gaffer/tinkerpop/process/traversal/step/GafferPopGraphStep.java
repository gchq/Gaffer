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

package uk.gov.gchq.gaffer.tinkerpop.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.GlobalViewElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph.HasStepFilterStage;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphVariables;
import uk.gov.gchq.gaffer.tinkerpop.process.traversal.step.util.GafferPopHasContainer;
import uk.gov.gchq.koryphe.impl.predicate.Exists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Custom GafferPop GraphStep provides Gaffer specific optimisations
 * for the initial GraphStep in a query.
 * Also responsible for parsing any options passed via a 'with()' step
 * on the query.
 *
 * <pre>
 * g.with("operationOptions", ["graphId:graph1", "opt1:val1"]).V()   // operation options extracted and applied
 * </pre>
 */
public class GafferPopGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder {

    private static final Logger LOGGER = LoggerFactory.getLogger(GafferPopGraphStep.class);

    private final List<HasContainer> hasContainers = new ArrayList<>();

    public GafferPopGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        LOGGER.debug("Running custom GraphStep on GafferPopGraph");
        originalGraphStep.getLabels().forEach(this::addLabel);

        // Save reference to the graph
        GafferPopGraph graph = (GafferPopGraph) originalGraphStep.getTraversal().getGraph().get();

        // Restore variables to defaults before parsing options
        graph.setDefaultVariables((GafferPopGraphVariables) graph.variables());

        // Find any options on the traversal
        Optional<OptionsStrategy> optionsStrategy = originalGraphStep.getTraversal().getStrategies().getStrategy(OptionsStrategy.class);
        if (optionsStrategy.isPresent()) {
            LOGGER.debug("Found options on requested traversal");
            optionsStrategy.get().getOptions().forEach((k, v) -> {
                if (graph.variables().asMap().containsKey(k)) {
                    graph.variables().set(k, v);
                }
            });
        }

        // Set the output iterator to the relevant filtered output from the class methods
        this.setIteratorSupplier(() ->
            (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices(graph) : this.edges(graph)));
    }

    private Iterator<? extends Edge> edges(final GafferPopGraph graph) {
        // Check for the labels being searched for to construct a View to filter with
        List<String> labels = getRequestedLabels();

        // Get the View needed to for the property predicates
        String filterStage = ((GafferPopGraphVariables) graph.variables()).getHasStepFilterStage();
        View view = createEdgeViewFromPredicates(filterStage, labels);

        if (view != null) {
            // Find using view to filter results
            return graph.edgesWithView(Arrays.asList(this.ids), Direction.BOTH, view);
        } else if (!labels.isEmpty()) {
            // Find using labels to filter results
            return graph.edges(Arrays.asList(this.ids), Direction.BOTH, labels.toArray(new String[0]));
        } else if (this.ids == null) {
            return Collections.emptyIterator();
        }

        // linear scan as fallback
        LOGGER.debug("Using fallback filter method: {} hasContainers found", hasContainers.size());
        return IteratorUtils.filter(graph.edges(this.ids), edge -> HasContainer.testAll(edge, hasContainers));
    }

    private Iterator<? extends Vertex> vertices(final GafferPopGraph graph) {
        // Check for the labels being searched for to construct a View to filter with
        List<String> labels = getRequestedLabels();

        // Get the View needed to for the property predicates
        String filterStage = ((GafferPopGraphVariables) graph.variables()).getHasStepFilterStage();
        View view = createVertexViewFromPredicates(filterStage, labels);

        if (view != null) {
            // Find using view to filter results
            return graph.verticesWithView(Arrays.asList(this.ids), view);
        } else if (!labels.isEmpty()) {
            // Find using labels to filter results
            return graph.vertices(Arrays.asList(this.ids), labels.toArray(new String[0]));
        } else if (this.ids == null) {
            return Collections.emptyIterator();
        }

        // linear scan as fallback
        LOGGER.debug("Using fallback filter method: {} hasContainers found", hasContainers.size());
        return IteratorUtils.filter(graph.vertices(this.ids), vertex -> HasContainer.testAll(vertex, hasContainers));
    }

    /**
     * Checks all the HasContainers to see what labels have been
     * requested for.
     *
     * @return List of labels requested in the {@link GraphStep}
     */
    private List<String> getRequestedLabels() {
        return hasContainers.stream()
            .filter(hc -> hc.getKey() != null && hc.getKey().equals(T.label.getAccessor()))
            .filter(hc -> Compare.eq == hc.getBiPredicate() || Contains.within == hc.getBiPredicate())
            .filter(hc -> hc.getValue() != null)
            // Incase of ~label.within([]) predicate
            // map to value list and then flatten
            .map(hc -> hc.getValue() instanceof List<?> ?
                (List<String>) hc.getValue() :
                Collections.singletonList((String) hc.getValue()))
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }


    /**
     * Creates a View from the predicates in the HasContainers.
     * This can be used to filter vertices.
     *
     * @param filterStage the stage to apply the filters
     * @param labels      the vertex labels to apply the filters to
     * @return View containing the filters
     */
    private View createVertexViewFromPredicates(final String filterStage, final List<String> labels) {
        ViewElementDefinition viewElementDefinition = createElementDefFromPredicates(filterStage, labels.isEmpty());
        if (viewElementDefinition == null) {
            return null;
        }

        View.Builder viewBuilder = new View.Builder();
        if (viewElementDefinition instanceof GlobalViewElementDefinition) {
            // Apply filters to all vertices
            viewBuilder.globalElements((GlobalViewElementDefinition) viewElementDefinition);
        } else {
            // Apply filters to each of the labels
            labels.forEach(l -> viewBuilder.entity(l, viewElementDefinition));
        }
        return viewBuilder.build();
    }

    /**
     * Creates a View from the predicates in the HasContainers.
     * This can be used to filter edges.
     *
     * @param filterStage the stage to apply the filters
     * @param labels      the edge labels to apply the filters to
     * @return View containing the filters
     */
    private View createEdgeViewFromPredicates(final String filterStage, final List<String> labels) {
        ViewElementDefinition viewElementDefinition = createElementDefFromPredicates(filterStage, labels.isEmpty());
        if (viewElementDefinition == null) {
            return null;
        }

        View.Builder viewBuilder = new View.Builder();
        if (viewElementDefinition instanceof GlobalViewElementDefinition) {
            // Apply filters to all edges
            viewBuilder.globalEdges((GlobalViewElementDefinition) viewElementDefinition);
        } else {
            // Apply filters to each of the labels
            labels.forEach(l -> viewBuilder.edge(l, viewElementDefinition));
        }
        return viewBuilder.build();
    }

    /**
     * Creates a ViewElementDefinition from the predicates in the HasContainers.
     * This can be used in a View to filter entities.
     *
     * @param filterStage  the stage to apply the filters
     * @param createGlobal whether to return a GlobalViewElementDefinition
     *                     or a ViewElementDefinition

     * @return ViewElementDefinition containing the filters
     */
    private ViewElementDefinition createElementDefFromPredicates(final String filterStage, final boolean createGlobal) {
        List<GafferPopHasContainer> predicateContainers = getRequestedPredicates();

        // No predicates found
        if (predicateContainers.isEmpty()) {
            return null;
        }

        // Add each predicate to the filter
        ElementFilter.Builder filterBuilder = new ElementFilter.Builder();
        predicateContainers.forEach(
            hc -> filterBuilder.select(hc.getKey())
                // Only apply the HC predicate to properties that exist
                .execute(new Exists())
                .select(hc.getKey())
                .execute(hc.getGafferPredicate()));
        ElementFilter elementFilter = filterBuilder.build();

        ViewElementDefinition.BaseBuilder<?> vBuilder = createGlobal ?
                new GlobalViewElementDefinition.Builder() :
                new ViewElementDefinition.Builder();

        // Decide when to apply the filter
        HasStepFilterStage hasStepFilterStage;
        try {
            hasStepFilterStage = HasStepFilterStage.valueOf(filterStage);
        } catch (final IllegalArgumentException e) {
            LOGGER.warn("Unknown hasStepFilterStage: {}. Defaulting to {}",
                filterStage, GafferPopGraph.DEFAULT_HAS_STEP_FILTER_STAGE);
            hasStepFilterStage = GafferPopGraph.DEFAULT_HAS_STEP_FILTER_STAGE;
        }

        switch (hasStepFilterStage) {
            case POST_TRANSFORM:
                vBuilder.postTransformFilter(elementFilter);
                break;
            case POST_AGGREGATION:
                vBuilder.postAggregationFilter(elementFilter);
                break;
            case PRE_AGGREGATION:
                vBuilder.preAggregationFilter(elementFilter);
                break;
            default:
                vBuilder.preAggregationFilter(elementFilter);
                break;
        }

        return vBuilder.build();
    }

    /**
     * Checks all the HasContainers to see which predicates have been requested
     *
     * @return List of HasContainers with predicates
     */
    private List<GafferPopHasContainer> getRequestedPredicates() {
        // Don't filter out null hc.getValue() incase of AndP/OrP
        return hasContainers.stream()
            .filter(hc -> hc.getKey() != null)
            .filter(hc -> !hc.getKey().equals(T.label.getAccessor()))
            .filter(hc -> !hc.getKey().equals(T.id.getAccessor()))
            .map(hc -> (GafferPopHasContainer) hc)
            .collect(Collectors.toList());
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(hasContainers);
    }

    @Override
    public void addHasContainer(final HasContainer original) {
        hasContainers.add(new GafferPopHasContainer(original));
    }
}
