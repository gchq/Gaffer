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
import org.apache.tinkerpop.gremlin.process.traversal.Text;
import org.apache.tinkerpop.gremlin.process.traversal.Text.RegexPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphVariables;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsIn;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Not;
import uk.gov.gchq.koryphe.impl.predicate.Regex;
import uk.gov.gchq.koryphe.impl.predicate.StringContains;
import uk.gov.gchq.koryphe.predicate.KoryphePredicate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * Custom GafferPop GraphStep provides Gaffer specific optimisations
 * for the initial GraphStep in a query. Also responsible for parsing
 * any options passed via a 'with()' call on the query.
 *
 * <pre>
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

        if (!labels.isEmpty()) {
            // Find using label to filter results
            return graph.edges(Arrays.asList(this.ids), Direction.BOTH, labels.toArray(new String[0]));
        }

        // linear scan as fallback
        return IteratorUtils.filter(graph.edges(this.ids), edge -> HasContainer.testAll(edge, hasContainers));
    }

    private Iterator<? extends Vertex> vertices(final GafferPopGraph graph) {
        // Check for the labels being searched for to construct a View to filter with
        List<String> labels = getRequestedLabels();

        // Get the ViewElementDefinition needed to for the property predicates
        ViewElementDefinition viewElementDefinition = createViewFromPredicates();

        if (viewElementDefinition != null) {
            return graph.verticesWithView(Arrays.asList(this.ids), viewElementDefinition, labels);
        } else if (!labels.isEmpty()) {
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

    /**
     * Creates a ViewElementDefinition from the predicates in the HasContainers.
     * This can be used in a View to filter entities.
     *
     * @return ViewElementDefinition containing the filters
     */
    private ViewElementDefinition createViewFromPredicates() {
        List<HasContainer> predicateContainers = getRequestedPredicates();

        // No predicates found
        if (predicateContainers.isEmpty()) {
            return null;
        }

        ElementFilter.Builder filterBuilder = new ElementFilter.Builder();
        predicateContainers
            .stream()
            .forEach(hc -> filterBuilder.select(hc.getKey()).execute(convertHasContainerToPredicate(hc)));

        return new ViewElementDefinition.Builder()
            .preAggregationFilter(filterBuilder.build())
            .build();
    }

    /**
     * Checks all the HasContainers to see what predicates have been requested
     *
     * @return List of HasContainers with predicates
     */
    private List<HasContainer> getRequestedPredicates() {
        return hasContainers.stream()
            .filter(hc -> hc.getKey() != null && !hc.getKey().equals(T.label.getAccessor()) && !hc.getKey().equals(T.id.getAccessor()))
            .filter(hc -> hc.getValue() != null)
            .collect(Collectors.toList());
    }

    /**
     * Converts a HasContainer containing a Tinkerpop Predicate {@link org.apache.tinkerpop.gremlin.process.traversal.P}
     * to a Koryphe Predicate that can be used in a Gaffer {@link ElementFilter}
     *
     * @param hc the HasContainer to convert
     * @return the KoryphePredicate
     */
    private KoryphePredicate<?> convertHasContainerToPredicate(final HasContainer hc) {
        BiPredicate<?, ?> p = hc.getBiPredicate();
        Object value = hc.getValue();

        if (p instanceof Compare) {
            return getComparePredicate((Compare) p, value);
        } else if (p instanceof Contains) {
            return getContainsPredicate((Contains) p, (Collection) value);
        } else if (p instanceof Text) {
            return getTextPredicate((Text) p, (String) value);
        } else if (p instanceof RegexPredicate) {
            return getRegexPredicate((RegexPredicate) p);
        }

        return null;
    }

    private KoryphePredicate<?> getComparePredicate(final Compare c, final Object value) {
        switch (c) {
            case eq:
                return new IsEqual(value);
            case neq:
                return new Not<Object>(new IsEqual(value));
            case gt:
                return new IsMoreThan((Comparable<?>) value);
            case gte:
                return new IsMoreThan((Comparable<?>) value, true);
            case lt:
                return new IsLessThan((Comparable<?>) value);
            case lte:
                return new IsLessThan((Comparable<?>) value, true);
            default:
                return null;
        }
    }

    private KoryphePredicate<?> getContainsPredicate(final Contains c, final Collection<Object> value) {
        switch (c) {
            case within:
                return new IsIn(value);
            case without:
                return new Not<Object>(new IsIn(value));
            default:
                return null;
        }
    }

    private KoryphePredicate<?> getTextPredicate(final Text t, final String value) {
        switch (t) {
            case startingWith:
                return new Regex("^" + value + ".*");
            case notStartingWith:
                return new Regex("^(?!" + value + ").*");
            case endingWith:
                return new Regex(".*" + value + "$");
            case notEndingWith:
                return new Regex(".*(?<!" + value + ")$");
            case containing:
                return new StringContains(value);
            case notContaining:
                return new Not<String>(new StringContains(value));
            default:
                return null;
        }
    }

    private KoryphePredicate<?> getRegexPredicate(final RegexPredicate p) {
        final Regex r = new Regex(p.getPattern());
        return p.isNegate() ? new Not<String>(r) : r;
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        if (hasContainer.getPredicate() instanceof AndP) {
            ((AndP<?>) hasContainer.getPredicate()).getPredicates().forEach(
                p -> this.addHasContainer(new HasContainer(hasContainer.getKey(), p)));
        } else {
            this.hasContainers.add(hasContainer);
        }
    }
}
