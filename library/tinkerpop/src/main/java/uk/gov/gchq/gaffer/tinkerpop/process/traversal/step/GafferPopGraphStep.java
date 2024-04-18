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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.Iterator;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;

/**
 * Custom GafferPop GraphStep provides Gaffer specific optimisations
 * for the initial GraphStep in a query. Also responsible for parsing
 * any options passed via a 'with()' call on the query.
 *
 * <pre>
 * @example
 * <p>
 * g.with("userId", "user").V()   // userId extracted to be used in the operation executions
 * g.with("dataAuths", "write-access,read-access").V()   // user access controls to apply on the user
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

        // Find any options on the traversal
        Optional<OptionsStrategy> optionsStrategy = originalGraphStep.getTraversal().getStrategies().getStrategy(OptionsStrategy.class);
        if (optionsStrategy.isPresent()) {
            optionsStrategy.get().getOptions().forEach((k, v) -> {
                if(graph.variables().asMap().containsKey(k)) {
                    graph.variables().set(k, v);
                }
            });
        }

        // Set the output iterator to the relevant filtered output from the class methods
        this.setIteratorSupplier(() ->
            (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices(graph) : this.edges(graph)));
    }

    private Iterator<? extends Edge> edges(final GafferPopGraph graph) {
        // Check for and labels being searched for to construct a View to filter with
        List<String> labels = getRequestedLabels();

        if (!labels.isEmpty()) {
            // Find using label to filter results
            return graph.edges(Arrays.asList(this.ids), labels.toArray(new String[0]));
        }

        // linear scan as fallback
        return IteratorUtils.filter(graph.edges(Arrays.asList(this.ids)), edge -> HasContainer.testAll(edge, hasContainers));
    }

    private Iterator<? extends Vertex> vertices(final GafferPopGraph graph) {
        // Check for and labels being searched for to construct a View to filter with
        List<String> labels = getRequestedLabels();

        if (!labels.isEmpty()) {
            // Find using label to filter results
            return graph.vertices(Arrays.asList(this.ids), labels.toArray(new String[0]));
        }

        // linear scan as fallback
        return IteratorUtils.filter(graph.vertices(Arrays.asList(this.ids)), vertex -> HasContainer.testAll(vertex, hasContainers));
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
            .filter(hc -> Compare.eq == hc.getBiPredicate())
            .filter(hc -> hc.getValue() != null)
            .map(hc -> (String) hc.getValue())
            .collect(Collectors.toList());
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(HasContainer hasContainer) {
        if (hasContainer.getPredicate() instanceof AndP) {
            ((AndP<?>) hasContainer.getPredicate()).getPredicates().forEach(
                p -> this.addHasContainer(new HasContainer(hasContainer.getKey(), p)));
        } else
            this.hasContainers.add(hasContainer);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }

}
